######################################################################################
# Project: Scrapper and processing of Peru's 2026 election candidate data
# Script: 5_export_album_bundle.R - Export album handoff bundle from processed data
# Author: Jorge Ruiz-Cabrejos and Inca Slop
# Created: 2026-03-26
# Last Updated: 2026-03-26
######################################################################################

#~#~#~#~#~#~#~#~#~#~#~#~#~#~#
rm(list = ls())
#~#~#~#~#~#~#~#~#~#~#~#~#~#~#


# ================================
# ==========  SETTINGS ===========
# ================================

REQUIRED_PACKAGES <- c(
  "readr",
  "dplyr",
  "stringr",
  "purrr",
  "tibble",
  "jsonlite"
)

CANDIDATES_CSV <- file.path("output", "candidatos.csv")
SENTENCES_CSV <- file.path("output", "relacion_sentencias.csv")
WORK_CSV <- file.path("output", "experiencia_laboral.csv")
IMAGE_ROOT <- file.path("output", "images")
QC_MANIFEST_CSV <- file.path("output", "average_faces", "manifests", "image_manifest.csv")

EXPORT_ROOT <- file.path("output", "album_export")
EXPORT_IMAGE_ROOT <- file.path(EXPORT_ROOT, "images", "candidates")
EXPORT_MANIFEST_ROOT <- file.path(EXPORT_ROOT, "manifests")
EXPORT_JSON_PATH <- file.path(EXPORT_ROOT, "candidates.json")
EXPORT_MANIFEST_PATH <- file.path(EXPORT_MANIFEST_ROOT, "export_manifest.csv")
PARTY_MANIFEST_PATH <- file.path(EXPORT_MANIFEST_ROOT, "party_manifest.csv")
IMPORT_GUIDE_PATH <- file.path(EXPORT_ROOT, "import_guide.md")

QC_EXPORT_COLUMNS <- c(
  "processing_status",
  "skip_reason",
  "alignment_mode",
  "face_count",
  "multi_face_flag",
  "blur_pct",
  "face_size_pct",
  "center_offset_pct",
  "contrast_pct",
  "exposure_score",
  "quality_score",
  "quality_band",
  "quality_exclusion_reason",
  "eligible_filtered",
  "eligible_all"
)


# =======================================
# ==========  CUSTOM FUNCTIONS ===========
# =======================================

source(file.path("scripts", "_functions.R"))


# ================================
# ==========  LIBRARIES ==========
# ================================

install_if_missing(REQUIRED_PACKAGES)

for (pkg_name in REQUIRED_PACKAGES) {
  suppressPackageStartupMessages(
    library(pkg_name, character.only = TRUE)
  )
}


# ======================================
# ==========  LOCAL HELPERS ============
# ======================================

log_message <- function(text) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), text))
}

abort_export <- function(text) {
  stop(text, call. = FALSE)
}

normalize_export_path <- function(path) {
  normalizePath(path, winslash = "/", mustWork = FALSE)
}

portable_rel_path <- function(...) {
  gsub("\\\\", "/", file.path(...))
}

trim_na_vec <- function(x) {
  x <- trimws(as.character(x))
  x[x == ""] <- NA_character_
  x
}

display_title <- function(x) {
  x <- value_or_na(x)

  if (is.na(x)) {
    return(NA_character_)
  }

  stringr::str_to_title(stringr::str_to_lower(x), locale = "es")
}

derive_id_from_image_file <- function(path) {
  file_name <- tools::file_path_sans_ext(basename(path))
  parts <- strsplit(file_name, "_", fixed = TRUE)[[1]]

  if (length(parts) < 2) {
    return(NA_character_)
  }

  paste(parts[1:2], collapse = "_")
}

parse_income_integer <- function(x) {
  x <- value_or_na(x)

  if (is.na(x)) {
    return(NA_integer_)
  }

  cleaned <- x %>%
    gsub("[^0-9,.-]", "", ., perl = TRUE) %>%
    gsub(",", "", ., fixed = TRUE)

  if (!nzchar(cleaned)) {
    return(NA_integer_)
  }

  numeric_value <- suppressWarnings(as.numeric(cleaned))

  if (is.na(numeric_value)) {
    return(NA_integer_)
  }

  as.integer(round(numeric_value))
}

bool_to_upper_vec <- function(x) {
  ifelse(isTRUE(x) | (!is.na(x) & x), "TRUE", "FALSE")
}

as_bool_text <- function(x) {
  x <- value_or_na(x)

  if (is.na(x)) {
    return(NA_character_)
  }

  if (toupper(x) %in% c("TRUE", "T", "1")) {
    return("TRUE")
  }

  if (toupper(x) %in% c("FALSE", "F", "0")) {
    return("FALSE")
  }

  NA_character_
}

ensure_required_file <- function(path) {
  if (!file.exists(path)) {
    abort_export(paste("Required input file not found:", path))
  }
}

first_non_missing_chr <- function(x) {
  x <- trim_na_vec(x)
  x <- x[!is.na(x)]

  if (length(x) == 0) {
    return(NA_character_)
  }

  x[[1]]
}

rebuild_export_dirs <- function() {
  export_root_norm <- normalize_export_path(EXPORT_ROOT)

  # Guard against accidental deletes outside the expected export folder before
  # rebuilding the handoff bundle from scratch.
  if (!grepl("/output/album_export$", export_root_norm)) {
    abort_export(paste("Refusing to rebuild unexpected export directory:", export_root_norm))
  }

  if (dir.exists(EXPORT_ROOT)) {
    unlink(EXPORT_ROOT, recursive = TRUE, force = TRUE)
  }

  dir.create(EXPORT_IMAGE_ROOT, recursive = TRUE, showWarnings = FALSE)
  dir.create(EXPORT_MANIFEST_ROOT, recursive = TRUE, showWarnings = FALSE)
}

build_portrait_lookup <- function(image_root) {
  # The export is driven by the portrait tree on disk because that is the most
  # reliable statement of which candidate images actually exist locally.
  files <- list.files(image_root, pattern = "\\.jpg$", recursive = TRUE, full.names = TRUE)

  if (length(files) == 0) {
    abort_export("No JPG portraits were found under output/images.")
  }

  root_norm <- normalize_export_path(image_root)

  lookup <- tibble::tibble(
    source_image_abs_path = normalize_export_path(files)
  ) %>%
    dplyr::mutate(
      source_image_relative_path = substring(.data$source_image_abs_path, nchar(root_norm) + 2L),
      source_id_candidato = purrr::map_chr(.data$source_image_abs_path, derive_id_from_image_file)
    )

  bad_ids <- lookup %>%
    dplyr::filter(is.na(.data$source_id_candidato))

  if (nrow(bad_ids) > 0) {
    abort_export("One or more portrait files could not be mapped to id_candidato.")
  }

  duplicate_ids <- lookup %>%
    dplyr::count(.data$source_id_candidato, name = "portrait_count") %>%
    dplyr::filter(.data$portrait_count > 1)

  if (nrow(duplicate_ids) > 0) {
    abort_export("Multiple portrait files were found for at least one id_candidato.")
  }

  lookup
}

build_qc_lookup <- function(path) {
  # QC data is optional. When it exists we enrich the export manifest, and when
  # it does not we still allow the bundle to be built with blank QC columns.
  if (!file.exists(path)) {
    out <- empty_chr_tibble(c("id_candidato", paste0("qc_", QC_EXPORT_COLUMNS)))
    return(list(data = out, available = FALSE))
  }

  qc_df <- read_csv_chr(path)

  if (is.null(qc_df) || nrow(qc_df) == 0) {
    out <- empty_chr_tibble(c("id_candidato", paste0("qc_", QC_EXPORT_COLUMNS)))
    return(list(data = out, available = FALSE))
  }

  missing_cols <- setdiff(QC_EXPORT_COLUMNS, names(qc_df))
  for (col_name in missing_cols) {
    qc_df[[col_name]] <- NA_character_
  }

  qc_lookup <- qc_df %>%
    dplyr::transmute(
      id_candidato = .data$id_candidato,
      qc_processing_status = trim_na_vec(.data$processing_status),
      qc_skip_reason = trim_na_vec(.data$skip_reason),
      qc_alignment_mode = trim_na_vec(.data$alignment_mode),
      qc_face_count = trim_na_vec(.data$face_count),
      qc_multi_face_flag = purrr::map_chr(.data$multi_face_flag, as_bool_text),
      qc_blur_pct = trim_na_vec(.data$blur_pct),
      qc_face_size_pct = trim_na_vec(.data$face_size_pct),
      qc_center_offset_pct = trim_na_vec(.data$center_offset_pct),
      qc_contrast_pct = trim_na_vec(.data$contrast_pct),
      qc_exposure_score = trim_na_vec(.data$exposure_score),
      qc_quality_score = trim_na_vec(.data$quality_score),
      qc_quality_band = trim_na_vec(.data$quality_band),
      qc_quality_exclusion_reason = trim_na_vec(.data$quality_exclusion_reason),
      qc_eligible_filtered = purrr::map_chr(.data$eligible_filtered, as_bool_text),
      qc_eligible_all = purrr::map_chr(.data$eligible_all, as_bool_text)
    ) %>%
    dplyr::filter(!is.na(.data$id_candidato)) %>%
    dplyr::distinct(.data$id_candidato, .keep_all = TRUE)

  list(data = qc_lookup, available = TRUE)
}

copy_export_portraits <- function(export_df) {
  copied <- 0L

  # Copy portraits into a flat, deterministic export layout so downstream tools
  # do not need to know the original district/party folder structure.
  for (row_idx in seq_len(nrow(export_df))) {
    source_path <- export_df$source_image_abs_path[[row_idx]]
    dest_path <- file.path(EXPORT_ROOT, export_df$stickerImage[[row_idx]])

    dir.create(dirname(dest_path), recursive = TRUE, showWarnings = FALSE)

    success <- file.copy(source_path, dest_path, overwrite = TRUE, copy.mode = TRUE)

    if (!isTRUE(success) || !file.exists(dest_path)) {
      abort_export(paste("Failed to copy portrait asset:", source_path))
    }

    copied <- copied + 1L
  }

  copied
}

build_import_guide <- function(
  export_count,
  party_count,
  image_count,
  qc_available,
  qc_matched_count,
  candidate_only_note = TRUE
) {
  qc_status_line <- if (isTRUE(qc_available)) {
    paste0(
      "- QC merge: `output/average_faces/manifests/image_manifest.csv` was found and matched `",
      qc_matched_count,
      "` candidate rows. The `qc_*` columns in `manifests/export_manifest.csv` come from that file."
    )
  } else {
    "- QC merge: no QC manifest was found. The export still succeeded and the `qc_*` columns were written blank."
  }

  party_note_line <- if (isTRUE(candidate_only_note)) {
    "- Party stickers: intentionally excluded in v1. `manifests/party_manifest.csv` is metadata-only for future expansion."
  } else {
    "- Party stickers: included."
  }

  paste(
    "# Congress Album Export Bundle",
    "",
    "## Snapshot",
    paste0("- Candidates exported: `", export_count, "`"),
    paste0("- Parties represented: `", party_count, "`"),
    paste0("- Portrait assets copied: `", image_count, "`"),
    "- Bundle root: `output/album_export/`",
    "",
    "## Files",
    "- `candidates.json`: album-ready sticker dataset using the `_examples/candidates.json` contract.",
    "- `images/candidates/`: flattened portrait assets referenced by `candidates.json`.",
    "- `manifests/export_manifest.csv`: owner-facing truth table with source fields, derived fields, and optional QC columns.",
    "- `manifests/party_manifest.csv`: party metadata with `partido_id`, display name, slug, candidate count, and `url_logo_partido`.",
    "",
    "## Field Mapping",
    "- `id`: `dip_<id_candidato>`.",
    "- `name`, `party`, `region`: title-cased display labels derived from `output/candidatos.csv`.",
    "- `bioShort`: `Diputados - <Region>` or `Diputados` when region is missing.",
    "- `sentenciado`: `TRUE` if the candidate appears in `output/relacion_sentencias.csv`, otherwise `FALSE`.",
    "- `trabaja`: `TRUE` if the candidate appears in `output/experiencia_laboral.csv`, otherwise `FALSE`.",
    "- `ingresos`: integer soles parsed from `ingresos_total_ingresos`; blank in the manifest and `null` in JSON when missing.",
    "- `stickerImage` and `portraitImage`: both point to the same copied JPG for fastest owner integration.",
    "",
    "## Notes",
    qc_status_line,
    party_note_line,
    "- Portrait inclusion is driven by the local `output/images/` tree, not `ruta_imagen`, because some candidate rows do not persist that field.",
    "- Sort order is deterministic: region, then party, then candidate name, then source ID.",
    sep = "\n"
  )
}


# ============================
# ==========  SETUP ==========
# ============================

create_env()

ensure_required_file(CANDIDATES_CSV)
ensure_required_file(SENTENCES_CSV)
ensure_required_file(WORK_CSV)


# ======================================
# ==========  LOAD INPUTS ==============
# ======================================

log_message("Loading core input tables.")

candidate_df <- read_csv_chr(CANDIDATES_CSV)
sentence_df <- read_csv_chr(SENTENCES_CSV)
work_df <- read_csv_chr(WORK_CSV)

if (is.null(candidate_df) || nrow(candidate_df) == 0) {
  abort_export("output/candidatos.csv is empty.")
}

candidate_df <- candidate_df %>%
  dplyr::filter(!is.na(.data$id_candidato)) %>%
  dplyr::distinct(.data$id_candidato, .keep_all = TRUE)

if (nrow(candidate_df) == 0) {
  abort_export("No candidate rows remained after removing empty ids.")
}

portrait_lookup <- build_portrait_lookup(IMAGE_ROOT)
qc_lookup <- build_qc_lookup(QC_MANIFEST_CSV)

sentence_ids <- sentence_df %>%
  dplyr::filter(!is.na(.data$id_candidato)) %>%
  dplyr::distinct(.data$id_candidato) %>%
  dplyr::mutate(has_sentence = TRUE)

work_ids <- work_df %>%
  dplyr::filter(!is.na(.data$id_candidato)) %>%
  dplyr::distinct(.data$id_candidato) %>%
  dplyr::mutate(has_work = TRUE)


# ======================================
# ==========  BUILD EXPORT =============
# ======================================

log_message("Joining candidates, portraits, and optional QC metadata.")

missing_portraits <- setdiff(candidate_df$id_candidato, portrait_lookup$source_id_candidato)
unknown_portraits <- setdiff(portrait_lookup$source_id_candidato, candidate_df$id_candidato)

if (length(missing_portraits) > 0) {
  abort_export(paste("Portrait files are missing for", length(missing_portraits), "candidate ids."))
}

if (length(unknown_portraits) > 0) {
  abort_export(paste("Portrait files were found for", length(unknown_portraits), "unknown candidate ids."))
}

export_df <- candidate_df %>%
  # Build one clean owner-facing row per candidate by joining the flat tables,
  # portrait inventory, and optional QC metadata.
  dplyr::inner_join(portrait_lookup, by = c("id_candidato" = "source_id_candidato")) %>%
  dplyr::left_join(sentence_ids, by = "id_candidato") %>%
  dplyr::left_join(work_ids, by = "id_candidato") %>%
  dplyr::left_join(qc_lookup$data, by = "id_candidato") %>%
  dplyr::mutate(
    export_id = paste0("dip_", .data$id_candidato),
    name = purrr::map_chr(.data$nombre, display_title),
    party = purrr::map_chr(.data$partido_politico, display_title),
    region = purrr::map_chr(.data$distrito_electoral, display_title),
    bioShort = dplyr::if_else(
      !is.na(.data$region) & nzchar(.data$region),
      paste0("Diputados - ", .data$region),
      "Diputados"
    ),
    sentenciado = dplyr::coalesce(.data$has_sentence, FALSE),
    trabaja = dplyr::coalesce(.data$has_work, FALSE),
    ingresos = purrr::map_int(.data$ingresos_total_ingresos, parse_income_integer),
    stickerImage = portable_rel_path("images", "candidates", paste0(.data$export_id, ".jpg")),
    portraitImage = .data$stickerImage
  ) %>%
  dplyr::arrange(.data$region, .data$party, .data$name, .data$id_candidato) %>%
  dplyr::mutate(sort_order = dplyr::row_number())

if (nrow(export_df) != nrow(candidate_df)) {
  abort_export("The export row count does not match the candidate table row count.")
}

if (any(duplicated(export_df$export_id))) {
  abort_export("Duplicate export IDs were generated.")
}

if (any(nchar(export_df$export_id) > 32L)) {
  abort_export("At least one export ID exceeds 32 characters.")
}

if (any(is.na(export_df$name) | is.na(export_df$party))) {
  abort_export("At least one candidate is missing a display name or party after normalization.")
}


# ======================================
# ==========  WRITE BUNDLE =============
# ======================================

log_message("Rebuilding output/album_export.")
rebuild_export_dirs()

copied_images <- copy_export_portraits(export_df)

json_df <- export_df %>%
  dplyr::transmute(
    id = .data$export_id,
    name = .data$name,
    party = .data$party,
    region = .data$region,
    bioShort = .data$bioShort,
    stickerImage = .data$stickerImage,
    portraitImage = .data$portraitImage,
    sentenciado = .data$sentenciado,
    ingresos = .data$ingresos,
    trabaja = .data$trabaja
  )

json_rows <- purrr::pmap(
  json_df,
  function(id, name, party, region, bioShort, stickerImage, portraitImage, sentenciado, ingresos, trabaja) {
    list(
      id = id,
      name = name,
      party = party,
      region = region,
      bioShort = bioShort,
      stickerImage = stickerImage,
      portraitImage = portraitImage,
      sentenciado = sentenciado,
      ingresos = ingresos,
      trabaja = trabaja
    )
  }
)

jsonlite::write_json(
  json_rows,
  path = EXPORT_JSON_PATH,
  pretty = TRUE,
  auto_unbox = TRUE,
  na = "null",
  null = "null"
)

export_manifest <- export_df %>%
  dplyr::transmute(
    sort_order = .data$sort_order,
    id = .data$export_id,
    source_id_candidato = .data$id_candidato,
    source_partido_id = .data$partido_id,
    name = .data$name,
    party = .data$party,
    region = .data$region,
    bioShort = .data$bioShort,
    stickerImage = .data$stickerImage,
    portraitImage = .data$portraitImage,
    sentenciado = bool_to_upper_vec(.data$sentenciado),
    ingresos = ifelse(is.na(.data$ingresos), NA_character_, as.character(.data$ingresos)),
    trabaja = bool_to_upper_vec(.data$trabaja),
    source_url_hoja_vida = .data$url_hoja_vida,
    source_url_imagen = .data$url_imagen,
    source_image_path = portable_rel_path("output", "images", .data$source_image_relative_path),
    source_raw_income_text = .data$ingresos_total_ingresos,
    source_region_raw = .data$distrito_electoral,
    source_party_raw = .data$partido_politico,
    source_nombre_raw = .data$nombre,
    qc_processing_status = .data$qc_processing_status,
    qc_skip_reason = .data$qc_skip_reason,
    qc_alignment_mode = .data$qc_alignment_mode,
    qc_face_count = .data$qc_face_count,
    qc_multi_face_flag = .data$qc_multi_face_flag,
    qc_blur_pct = .data$qc_blur_pct,
    qc_face_size_pct = .data$qc_face_size_pct,
    qc_center_offset_pct = .data$qc_center_offset_pct,
    qc_contrast_pct = .data$qc_contrast_pct,
    qc_exposure_score = .data$qc_exposure_score,
    qc_quality_score = .data$qc_quality_score,
    qc_quality_band = .data$qc_quality_band,
    qc_quality_exclusion_reason = .data$qc_quality_exclusion_reason,
    qc_eligible_filtered = .data$qc_eligible_filtered,
    qc_eligible_all = .data$qc_eligible_all
  )

write_csv_chr(export_manifest, EXPORT_MANIFEST_PATH)

party_manifest <- export_df %>%
  dplyr::transmute(
    partido_id = .data$partido_id,
    party = .data$party,
    party_slug = purrr::map_chr(.data$party, slugify_path),
    url_logo_partido = .data$url_logo_partido
  ) %>%
  dplyr::group_by(.data$partido_id, .data$party, .data$party_slug) %>%
  dplyr::summarise(
    candidate_count = dplyr::n(),
    url_logo_partido = first_non_missing_chr(.data$url_logo_partido),
    .groups = "drop"
  ) %>%
  dplyr::arrange(.data$party, .data$partido_id)

write_csv_chr(party_manifest, PARTY_MANIFEST_PATH)

qc_matched_count <- export_manifest %>%
  dplyr::filter(!is.na(.data$qc_processing_status) | !is.na(.data$qc_quality_score) | !is.na(.data$qc_eligible_all)) %>%
  nrow()

writeLines(
  build_import_guide(
    export_count = nrow(export_df),
    party_count = nrow(party_manifest),
    image_count = copied_images,
    qc_available = qc_lookup$available,
    qc_matched_count = qc_matched_count
  ),
  IMPORT_GUIDE_PATH,
  useBytes = TRUE
)


# ======================================
# ==========  VALIDATION ===============
# ======================================

log_message("Running export validations.")

if (copied_images != nrow(export_df)) {
  abort_export("The number of copied images does not match the export row count.")
}

export_asset_exists <- file.exists(file.path(EXPORT_ROOT, export_df$stickerImage))
if (!all(export_asset_exists)) {
  abort_export("At least one exported portrait asset is missing after copy.")
}

json_ids_unique <- length(unique(json_df$id)) == nrow(json_df)
if (!isTRUE(json_ids_unique)) {
  abort_export("JSON IDs are not unique.")
}

log_message(paste("Candidates exported:", nrow(export_df)))
log_message(paste("Party manifest rows:", nrow(party_manifest)))
log_message(paste("Portrait assets copied:", copied_images))
log_message(paste("QC manifest detected:", if (isTRUE(qc_lookup$available)) "yes" else "no"))
log_message(paste("Album export written to:", normalize_export_path(EXPORT_ROOT)))
