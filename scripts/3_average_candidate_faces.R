######################################################################################
# Project: Scrapper and processing of Peru's 2026 election candidate data
# Script: 3_average_candidate_faces.R - Portrait QC and average-face generation
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

INPUT_IMAGE_ROOT <- file.path("output", "images")
CANDIDATES_CSV <- file.path("output", "candidatos.csv")
OUTPUT_ROOT <- file.path("output", "average_faces")
FILTERED_ROOT <- file.path(OUTPUT_ROOT, "filtered")
ALL_FACES_ROOT <- file.path(OUTPUT_ROOT, "all_faces")
MANIFEST_ROOT <- file.path(OUTPUT_ROOT, "manifests")
FIRST_NAME_LOOKUP_CSV <- file.path("data", "reference", "first_name_sex.csv")
SEX_OVERRIDE_CSV <- file.path("data", "manual", "sex_overrides.csv")

BLEND_MODES <- c("filtered", "all_faces")
GROUP_MODES <- c("all", "sex", "region", "region_sex", "affiliation", "region_affiliation")

MIN_VALID_IMAGES <- 3L
TARGET_WIDTH <- 427L
TARGET_HEIGHT <- 602L
MAX_IMAGES_PER_GROUP <- NA_integer_
MAX_QC_PREVIEW_IMAGES <- 35L
SAVE_QC_MONTAGES <- TRUE
QUALITY_THRESHOLD <- 0.45

REQUIRED_PACKAGES <- c(
  "readr",
  "dplyr",
  "stringr",
  "purrr",
  "tibble",
  "magick",
  "opencv"
)

PACKAGE_REPOS <- c(
  "https://ropensci.r-universe.dev",
  "https://cloud.r-project.org"
)


# =======================================
# ==========  CUSTOM FUNCTIONS ===========
# =======================================

source(file.path("scripts", "_functions.R"))


# ================================
# ==========  LIBRARIES ==========
# ================================

install_required_packages_average <- function(pkgs) {
  target_lib <- file.path(getwd(), "r_libs")

  # Keep image-heavy dependencies inside the project so the script does not rely
  # on whatever happens to be installed globally on the machine.
  Sys.setenv(R_LIBS_USER = target_lib)

  if (!dir.exists(target_lib)) {
    dir.create(target_lib, recursive = TRUE, showWarnings = FALSE)
  }

  .libPaths(unique(c(target_lib, .libPaths())))
  installed <- rownames(installed.packages())
  missing_pkgs <- pkgs[!pkgs %in% installed]

  if (length(missing_pkgs) > 0) {
    install.packages(missing_pkgs, repos = PACKAGE_REPOS, lib = target_lib)
  }
}

install_required_packages_average(REQUIRED_PACKAGES)

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

normalize_fs_path <- function(path) {
  normalizePath(path, winslash = "/", mustWork = FALSE)
}

trim_na_chr <- function(x) {
  x <- trimws(as.character(x))
  x[x == ""] <- NA_character_
  x
}

label_from_slug <- function(x) {
  x <- value_or_na(x)

  if (is.na(x)) {
    return(NA_character_)
  }

  x %>%
    gsub("-", " ", ., fixed = TRUE) %>%
    tools::toTitleCase()
}

canonical_name_key <- function(x) {
  x <- trim_na_chr(x)

  if (length(x) == 0 || is.na(x[[1]])) {
    return(NA_character_)
  }

  x <- iconv(x[[1]], to = "ASCII//TRANSLIT")
  x <- toupper(x)
  x <- gsub("[^A-Z0-9 ]+", " ", x, perl = TRUE)
  x <- gsub("\\s+", " ", x, perl = TRUE)
  x <- trimws(x)

  if (!nzchar(x)) {
    return(NA_character_)
  }

  x
}

normalize_sex_value <- function(x) {
  x <- canonical_name_key(x)

  if (is.na(x)) {
    return(NA_character_)
  }

  if (x %in% c("F", "FEMENINO", "FEMALE", "MUJER")) {
    return("female")
  }

  if (x %in% c("M", "MASCULINO", "MALE", "HOMBRE", "VARON")) {
    return("male")
  }

  if (grepl("FEM|MUJER", x)) {
    return("female")
  }

  if (grepl("MASC|HOMBRE|VARON", x)) {
    return("male")
  }

  NA_character_
}

sex_display_label <- function(x) {
  if (identical(x, "female")) {
    return("Female")
  }

  if (identical(x, "male")) {
    return("Male")
  }

  NA_character_
}

derive_candidate_id_from_file <- function(file_name) {
  parts <- strsplit(tools::file_path_sans_ext(basename(file_name)), "_", fixed = TRUE)[[1]]

  if (length(parts) < 2) {
    return(NA_character_)
  }

  paste(parts[1:2], collapse = "_")
}

derive_candidate_name_from_file <- function(file_name) {
  parts <- strsplit(tools::file_path_sans_ext(basename(file_name)), "_", fixed = TRUE)[[1]]

  if (length(parts) <= 2) {
    return(NA_character_)
  }

  label_from_slug(paste(parts[-c(1, 2)], collapse = "-"))
}

empty_candidate_metadata <- function() {
  tibble::tibble(
    id_candidato = character(),
    nombre = character(),
    sexo_raw = character(),
    distrito_electoral = character(),
    partido_politico = character(),
    csv_region_slug = character(),
    csv_affiliation_slug = character()
  )
}

load_candidate_metadata <- function(path) {
  candidate_df <- read_csv_chr(path)

  if (is.null(candidate_df) || nrow(candidate_df) == 0) {
    return(empty_candidate_metadata())
  }

  # Normalize the metadata early so later QC and grouping steps can trust the
  # same region, party, and candidate-id fields everywhere.
  candidate_df %>%
    dplyr::transmute(
      id_candidato = trim_na_chr(.data$id_candidato),
      nombre = trim_na_chr(.data$nombre),
      sexo_raw = trim_na_chr(.data$sexo),
      distrito_electoral = trim_na_chr(.data$distrito_electoral),
      partido_politico = trim_na_chr(.data$partido_politico),
      csv_region_slug = purrr::map_chr(.data$distrito_electoral, slugify_path),
      csv_affiliation_slug = purrr::map_chr(.data$partido_politico, slugify_path)
    ) %>%
    dplyr::filter(!is.na(.data$id_candidato)) %>%
    dplyr::distinct(.data$id_candidato, .keep_all = TRUE)
}

empty_lookup_tibble <- function() {
  tibble::tibble(
    name_key = character(),
    sex_assigned = character(),
    sex_confidence = character(),
    sex_note = character()
  )
}

load_first_name_lookup <- function(path) {
  lookup_df <- read_csv_chr(path)

  if (is.null(lookup_df) || nrow(lookup_df) == 0) {
    return(empty_lookup_tibble())
  }

  lookup_df %>%
    dplyr::transmute(
      name_key = purrr::map_chr(.data$name_key, canonical_name_key),
      sex_assigned = purrr::map_chr(.data$sex_assigned, normalize_sex_value),
      sex_confidence = trim_na_chr(.data$sex_confidence),
      sex_note = trim_na_chr(.data$sex_note)
    ) %>%
    dplyr::filter(!is.na(.data$name_key), !is.na(.data$sex_assigned)) %>%
    dplyr::distinct(.data$name_key, .keep_all = TRUE)
}

empty_override_tibble <- function() {
  tibble::tibble(
    id_candidato = character(),
    sex_assigned = character(),
    sex_confidence = character(),
    sex_note = character()
  )
}

load_sex_overrides <- function(path) {
  override_df <- read_csv_chr(path)

  if (is.null(override_df) || nrow(override_df) == 0) {
    return(empty_override_tibble())
  }

  override_df %>%
    dplyr::transmute(
      id_candidato = trim_na_chr(.data$id_candidato),
      sex_assigned = purrr::map_chr(.data$sex_assigned, normalize_sex_value),
      sex_confidence = trim_na_chr(.data$sex_confidence),
      sex_note = trim_na_chr(.data$sex_note)
    ) %>%
    dplyr::filter(!is.na(.data$id_candidato), !is.na(.data$sex_assigned)) %>%
    dplyr::mutate(
      sex_confidence = dplyr::coalesce(.data$sex_confidence, "high")
    ) %>%
    dplyr::distinct(.data$id_candidato, .keep_all = TRUE)
}

name_lookup_skip_tokens <- c(
  "DA", "DAS", "DE", "DEL", "DI", "DO", "DOS", "EL", "LA", "LAS", "LOS", "Y"
)

female_fallback_tokens <- c(
  "ADOLFA", "ANA", "ANITA", "ANTONIA", "BERTHA", "BETTY", "BLANCA", "CARMEN",
  "CAROL", "CLAUDIA", "DEYSI", "DIANA", "DINA", "DORIS", "EDITH", "ELIZABETH",
  "ELVA", "ELVIA", "EMMA", "ERIKA", "ESMERALDA", "ESTHER", "FATIMA", "FAUSTINA",
  "FLOR", "GLADYS", "GLORIA", "GRACIELA", "INES", "JANET", "JANETH", "JENNY",
  "JESSICA", "JUANA", "KAREN", "KARIN", "KARINA", "KATIA", "KELLY", "LILIANA",
  "LILY", "LIZ", "LIZBETH", "LORENA", "LOURDES", "LUCIANA", "LUCIA", "LUISA",
  "LUZ", "LUZMILA", "MAGALI", "MAGALY", "MARCELA", "MARIA", "MARIELA", "MARILU",
  "MARITZA", "MARLENE", "MARLENY", "MARTHA", "MARY", "MERCEDES", "MERY",
  "MILAGROS", "MIRIAM", "MORELIA", "NANCY", "NELLY", "NORMA", "OLGA", "PAOLA",
  "PATRICIA", "PAULA", "PILAR", "RAQUEL", "ROCIO", "ROSA", "ROSARIO", "ROXANA",
  "RUTH", "SANDRA", "SHEYLA", "SHIRLEY", "SILVIA", "SOFIA", "SONIA", "SUSAN",
  "TANIA", "TERESA", "TOMASA", "VANESSA", "VILMA", "VIRGINIA", "YAHAIRA",
  "YENNY", "YESSENIA", "YESENIA", "YSABEL", "YLDA", "ZENOBIA"
)

male_fallback_tokens <- c(
  "ABEL", "ALBERTO", "ALCIDES", "ALEJANDRO", "ALFONSO", "ALFREDO", "ALIPIO",
  "ANDRES", "ANGEL", "ANIBAL", "ANTONIO", "ARTURO", "BERNARDINO", "CARLOS",
  "CESAR", "CLAUDIO", "CRISTOBAL", "DANIEL", "DANTE", "DAVID", "DENNIS",
  "DOMINGO", "EDGAR", "EDILBERTO", "EDUARDO", "EDWIN", "ELVIS", "EMERSON",
  "ENRIQUE", "FELIX", "FERNANDO", "FRANCISCO", "FRANKLIN", "FREDY", "GABRIEL",
  "GEINER", "GERMAN", "GILMAR", "GODOFREDO", "GUIDO", "GUSTAVO", "HEBER",
  "HECTOR", "HENRY", "HERNAN", "HUGO", "IVAN", "JAIME", "JAVIER", "JEFFERSON",
  "JHON", "JHONY", "JHONATAN", "JORGE", "JOSE", "JUAN", "JULIO", "LUIS",
  "MANUEL", "MARAT", "MARCO", "MARIANO", "MARIO", "MICHAEL", "MIGUEL",
  "MILTON", "MOISES", "NELSON", "NICOLAS", "OSCAR", "PABLO", "PERCY", "PEDRO",
  "RAFAEL", "RAUL", "RICARDO", "ROBERTO", "ROGER", "ROLANDO", "RONALD",
  "SEGUNDO", "VICTOR", "WALTER", "WILFREDO", "WILSER", "YONI"
)

female_suffix_exceptions <- c("NOA")
male_suffix_exceptions <- c("ROCIO", "ROSARIO", "CONSUELO")

candidate_name_keys <- function(name) {
  tokens <- canonical_name_key(name)

  if (is.na(tokens)) {
    return(character())
  }

  tokens <- unlist(strsplit(tokens, " ", fixed = TRUE), use.names = FALSE)
  tokens <- tokens[nzchar(tokens)]
  tokens <- tokens[!tokens %in% name_lookup_skip_tokens]

  if (length(tokens) == 0) {
    return(character())
  }

  key_candidates <- c(
    if (length(tokens) >= 2) paste(tokens[1], tokens[2]) else character(),
    if (length(tokens) >= 3) paste(tokens[2], tokens[3]) else character(),
    tokens[1],
    if (length(tokens) >= 2) tokens[2] else character(),
    if (length(tokens) >= 3) tokens[3] else character()
  )

  unique(key_candidates[nzchar(key_candidates)])
}

infer_sex_from_name <- function(name, lookup_df) {
  empty_result <- list(
    sex_assigned = NA_character_,
    sex_source = NA_character_,
    sex_confidence = NA_character_,
    sex_note = NA_character_,
    matched_name_key = NA_character_,
    needs_manual_review = TRUE
  )

  name_keys <- candidate_name_keys(name)

  if (length(name_keys) == 0) {
    return(empty_result)
  }

  if (nrow(lookup_df) > 0) {
    for (name_key in name_keys) {
      hit <- lookup_df[lookup_df$name_key == name_key, , drop = FALSE]

      if (nrow(hit) > 0) {
        return(list(
          sex_assigned = hit$sex_assigned[[1]],
          sex_source = if (grepl(" ", name_key, fixed = TRUE)) "first_name_lookup_compound" else "first_name_lookup",
          sex_confidence = coalesce_chr(hit$sex_confidence[[1]], "high"),
          sex_note = hit$sex_note[[1]],
          matched_name_key = name_key,
          needs_manual_review = FALSE
        ))
      }
    }
  }

  primary_key <- name_keys[[1]]
  fallback_tokens <- name_keys[!grepl(" ", name_keys, fixed = TRUE)]

  for (token in fallback_tokens) {
    if (token %in% female_fallback_tokens) {
      return(list(
        sex_assigned = "female",
        sex_source = "heuristic_name_token",
        sex_confidence = "medium",
        sex_note = paste("Fallback token match:", token),
        matched_name_key = token,
        needs_manual_review = TRUE
      ))
    }

    if (token %in% male_fallback_tokens) {
      return(list(
        sex_assigned = "male",
        sex_source = "heuristic_name_token",
        sex_confidence = "medium",
        sex_note = paste("Fallback token match:", token),
        matched_name_key = token,
        needs_manual_review = TRUE
      ))
    }
  }

  if (!is.na(primary_key) && !primary_key %in% female_suffix_exceptions &&
      grepl("(A|IA|NA|ELA|INA|LUZ|YS|IZ)$", primary_key)) {
    return(list(
      sex_assigned = "female",
      sex_source = "heuristic_suffix",
      sex_confidence = "low",
      sex_note = paste("Suffix fallback from token:", primary_key),
      matched_name_key = primary_key,
      needs_manual_review = TRUE
    ))
  }

  if (!is.na(primary_key) && !primary_key %in% male_suffix_exceptions &&
      grepl("(O|ER|EL|ON|AN|EN|IN|AR|UR|OR|IO)$", primary_key)) {
    return(list(
      sex_assigned = "male",
      sex_source = "heuristic_suffix",
      sex_confidence = "low",
      sex_note = paste("Suffix fallback from token:", primary_key),
      matched_name_key = primary_key,
      needs_manual_review = TRUE
    ))
  }

  final_guess <- if (!is.na(primary_key) && grepl("A$", primary_key) && !primary_key %in% female_suffix_exceptions) {
    "female"
  } else {
    "male"
  }

  list(
    sex_assigned = final_guess,
    sex_source = "heuristic_last_resort",
    sex_confidence = "low",
    sex_note = paste("Last-resort fallback from token:", primary_key),
    matched_name_key = primary_key,
    needs_manual_review = TRUE
  )
}

assign_candidate_sex <- function(candidate_metadata, first_name_lookup, sex_overrides) {
  override_lookup <- if (nrow(sex_overrides) == 0) {
    sex_overrides
  } else {
    sex_overrides %>%
      dplyr::distinct(.data$id_candidato, .keep_all = TRUE)
  }

  assigned_rows <- purrr::map(seq_len(nrow(candidate_metadata)), function(row_idx) {
    candidate_row <- candidate_metadata[row_idx, , drop = FALSE]
    id_candidato <- row_value_or_na(candidate_row, "id_candidato")
    nombre <- coalesce_chr(
      row_value_or_na(candidate_row, "nombre"),
      derive_candidate_name_from_file(id_candidato)
    )
    sexo_raw <- row_value_or_na(candidate_row, "sexo_raw")
    official_value <- normalize_sex_value(sexo_raw)

    if (!is.na(official_value)) {
      return(tibble::tibble(
        id_candidato = id_candidato,
        nombre = nombre,
        sexo_raw = sexo_raw,
        sex_assigned = official_value,
        sex_source = "official_declared",
        sex_confidence = "high",
        sex_note = NA_character_,
        matched_name_key = NA_character_,
        needs_manual_review = FALSE
      ))
    }

    override_row <- override_lookup[override_lookup$id_candidato == id_candidato, , drop = FALSE]

    if (nrow(override_row) > 0) {
      return(tibble::tibble(
        id_candidato = id_candidato,
        nombre = nombre,
        sexo_raw = sexo_raw,
        sex_assigned = override_row$sex_assigned[[1]],
        sex_source = "manual_visual_override",
        sex_confidence = coalesce_chr(override_row$sex_confidence[[1]], "high"),
        sex_note = override_row$sex_note[[1]],
        matched_name_key = NA_character_,
        needs_manual_review = FALSE
      ))
    }

    inferred <- infer_sex_from_name(nombre, first_name_lookup)

    tibble::tibble(
      id_candidato = id_candidato,
      nombre = nombre,
      sexo_raw = sexo_raw,
      sex_assigned = inferred$sex_assigned,
      sex_source = inferred$sex_source,
      sex_confidence = inferred$sex_confidence,
      sex_note = inferred$sex_note,
      matched_name_key = inferred$matched_name_key,
      needs_manual_review = isTRUE(inferred$needs_manual_review)
    )
  })

  candidate_metadata %>%
    dplyr::select(
      "id_candidato",
      "nombre",
      "sexo_raw",
      "distrito_electoral",
      "partido_politico",
      "csv_region_slug",
      "csv_affiliation_slug"
    ) %>%
    dplyr::left_join(
      dplyr::bind_rows(assigned_rows),
      by = c("id_candidato", "nombre", "sexo_raw")
    ) %>%
    dplyr::mutate(
      sex_assigned = dplyr::coalesce(.data$sex_assigned, "male"),
      sex_source = dplyr::coalesce(.data$sex_source, "heuristic_last_resort"),
      sex_confidence = dplyr::coalesce(.data$sex_confidence, "low"),
      needs_manual_review = dplyr::coalesce(.data$needs_manual_review, TRUE)
    )
}

discover_portrait_manifest <- function(image_root, candidate_metadata) {
  files <- list.files(
    image_root,
    pattern = "\\.jpg$",
    recursive = TRUE,
    full.names = TRUE,
    ignore.case = TRUE
  )

  if (length(files) == 0) {
    stop("No portrait JPG files were found under output/images.", call. = FALSE)
  }

  root_norm <- normalize_fs_path(image_root)

  tibble::tibble(
    image_path = sort(normalize_fs_path(files))
  ) %>%
    dplyr::mutate(
      relative_path = substring(.data$image_path, nchar(root_norm) + 2L),
      path_parts = strsplit(.data$relative_path, "/", fixed = TRUE),
      region_slug = purrr::map_chr(.data$path_parts, ~ if (length(.x) >= 3) value_or_na(.x[1]) else NA_character_),
      affiliation_slug = purrr::map_chr(.data$path_parts, ~ if (length(.x) >= 3) value_or_na(.x[2]) else NA_character_),
      file_name = basename(.data$image_path),
      id_candidato = purrr::map_chr(.data$file_name, derive_candidate_id_from_file)
    ) %>%
    dplyr::select(-"path_parts") %>%
    dplyr::left_join(candidate_metadata, by = "id_candidato") %>%
    dplyr::mutate(
      nombre = purrr::pmap_chr(
        list(.data$nombre, .data$file_name),
        function(csv_value, file_name) coalesce_chr(csv_value, derive_candidate_name_from_file(file_name))
      ),
      region_label = purrr::pmap_chr(
        list(.data$distrito_electoral, .data$region_slug),
        function(csv_value, slug_value) coalesce_chr(csv_value, label_from_slug(slug_value))
      ),
      affiliation_label = purrr::pmap_chr(
        list(.data$partido_politico, .data$affiliation_slug),
        function(csv_value, slug_value) coalesce_chr(csv_value, label_from_slug(slug_value))
      ),
      metadata_region_conflict = !is.na(.data$csv_region_slug) & !is.na(.data$region_slug) & .data$csv_region_slug != .data$region_slug,
      metadata_affiliation_conflict = !is.na(.data$csv_affiliation_slug) & !is.na(.data$affiliation_slug) & .data$csv_affiliation_slug != .data$affiliation_slug,
      metadata_conflict = .data$metadata_region_conflict | .data$metadata_affiliation_conflict,
      processing_status = "pending",
      skip_reason = NA_character_,
      alignment_mode = NA_character_,
      aligned_cache_path = NA_character_,
      face_count = NA_integer_,
      selected_face_x = NA_real_,
      selected_face_y = NA_real_,
      selected_face_radius = NA_real_,
      source_width = NA_integer_,
      source_height = NA_integer_,
      crop_width = NA_integer_,
      crop_height = NA_integer_,
      aligned_width = NA_integer_,
      aligned_height = NA_integer_,
      multi_face_flag = FALSE,
      blur_score_raw = NA_real_,
      face_size_ratio = NA_real_,
      center_offset_ratio = NA_real_,
      luma_mean = NA_real_,
      luma_sd = NA_real_,
      blur_pct = NA_real_,
      face_size_pct = NA_real_,
      contrast_pct = NA_real_,
      center_offset_pct = NA_real_,
      exposure_score = NA_real_,
      quality_score = NA_real_,
      quality_band = NA_character_,
      quality_exclusion_reason = NA_character_,
      eligible_filtered = FALSE,
      eligible_all = FALSE
    )
}

fill_missing_portrait_sex_assignments <- function(image_manifest, first_name_lookup) {
  missing_idx <- is.na(image_manifest$sex_assigned) | !nzchar(image_manifest$sex_assigned)

  if (!any(missing_idx)) {
    return(image_manifest)
  }

  fallback_rows <- purrr::map(which(missing_idx), function(row_idx) {
    manifest_row <- image_manifest[row_idx, , drop = FALSE]
    inferred <- infer_sex_from_name(row_value_or_na(manifest_row, "nombre"), first_name_lookup)

    tibble::tibble(
      row_idx = row_idx,
      sex_assigned = dplyr::coalesce(inferred$sex_assigned, "male"),
      sex_source = dplyr::coalesce(inferred$sex_source, "heuristic_last_resort"),
      sex_confidence = dplyr::coalesce(inferred$sex_confidence, "low"),
      sex_note = dplyr::coalesce(
        inferred$sex_note,
        "Filled from portrait-manifest fallback because candidate metadata was missing."
      )
    )
  }) %>%
    dplyr::bind_rows()

  image_manifest$sex_assigned[fallback_rows$row_idx] <- fallback_rows$sex_assigned
  image_manifest$sex_source[fallback_rows$row_idx] <- fallback_rows$sex_source
  image_manifest$sex_confidence[fallback_rows$row_idx] <- fallback_rows$sex_confidence
  image_manifest$sex_note[fallback_rows$row_idx] <- fallback_rows$sex_note

  image_manifest
}

metadata_conflict_detail <- function(image_row) {
  pieces <- c(
    paste0("folder_region_slug=", coalesce_chr(row_value_or_na(image_row, "region_slug"), "NA")),
    paste0("csv_region_slug=", coalesce_chr(row_value_or_na(image_row, "csv_region_slug"), "NA")),
    paste0("folder_affiliation_slug=", coalesce_chr(row_value_or_na(image_row, "affiliation_slug"), "NA")),
    paste0("csv_affiliation_slug=", coalesce_chr(row_value_or_na(image_row, "csv_affiliation_slug"), "NA"))
  )

  paste(pieces, collapse = "; ")
}

empty_issue_tibble <- function() {
  tibble::tibble(
    timestamp = character(),
    id_candidato = character(),
    nombre = character(),
    image_path = character(),
    region_slug = character(),
    affiliation_slug = character(),
    issue_type = character(),
    detail = character(),
    face_count = character(),
    alignment_mode = character()
  )
}

build_issue_row <- function(
  image_row,
  issue_type,
  detail = NA_character_,
  face_count = NA_integer_,
  alignment_mode = NA_character_
) {
  tibble::tibble(
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    id_candidato = row_value_or_na(image_row, "id_candidato"),
    nombre = row_value_or_na(image_row, "nombre"),
    image_path = row_value_or_na(image_row, "image_path"),
    region_slug = row_value_or_na(image_row, "region_slug"),
    affiliation_slug = row_value_or_na(image_row, "affiliation_slug"),
    issue_type = coalesce_chr(issue_type),
    detail = value_or_na(detail),
    face_count = if (is.na(face_count)) NA_character_ else as.character(face_count),
    alignment_mode = value_or_na(alignment_mode)
  )
}

clip_crop_spec <- function(crop_left, crop_top, crop_width, crop_height, image_width, image_height) {
  clip_left <- max(0L, as.integer(crop_left))
  clip_top <- max(0L, as.integer(crop_top))
  clip_right <- min(as.integer(image_width), as.integer(crop_left + crop_width))
  clip_bottom <- min(as.integer(image_height), as.integer(crop_top + crop_height))
  clip_width <- max(1L, as.integer(clip_right - clip_left))
  clip_height <- max(1L, as.integer(clip_bottom - clip_top))

  list(
    crop_left = as.integer(crop_left),
    crop_top = as.integer(crop_top),
    crop_width = as.integer(crop_width),
    crop_height = as.integer(crop_height),
    clip_left = clip_left,
    clip_top = clip_top,
    clip_width = clip_width,
    clip_height = clip_height,
    offset_x = clip_left - as.integer(crop_left),
    offset_y = clip_top - as.integer(crop_top)
  )
}

compute_crop_spec <- function(image_width, image_height, face_row) {
  radius_value <- max(1, round(as.numeric(face_row[["radius"]])))
  face_x <- as.numeric(face_row[["x"]])
  face_y <- as.numeric(face_row[["y"]]) + (0.35 * radius_value)

  crop_width <- max(1L, as.integer(round(3.4 * radius_value)))
  crop_height <- max(1L, as.integer(round(4.8 * radius_value)))

  crop_left <- as.integer(floor(face_x - (crop_width / 2)))
  crop_top <- as.integer(floor(face_y - (crop_height / 2)))

  c(
    list(
      face_x = face_x,
      face_y = face_y,
      face_radius = radius_value
    ),
    clip_crop_spec(crop_left, crop_top, crop_width, crop_height, image_width, image_height)
  )
}

compute_portrait_prior_crop <- function(image_width, image_height) {
  target_ratio <- TARGET_WIDTH / TARGET_HEIGHT
  image_width <- as.integer(image_width)
  image_height <- as.integer(image_height)

  crop_width <- image_width
  crop_height <- as.integer(round(crop_width / target_ratio))

  if (crop_height > image_height) {
    crop_height <- image_height
    crop_width <- as.integer(round(crop_height * target_ratio))
  }

  crop_width <- max(1L, min(crop_width, image_width))
  crop_height <- max(1L, min(crop_height, image_height))

  crop_left <- as.integer(floor((image_width - crop_width) / 2))
  crop_top <- as.integer(floor((image_height - crop_height) * 0.15))
  crop_top <- max(0L, min(crop_top, image_height - crop_height))

  approx_radius <- max(1, min(
    as.integer(round(crop_width / 3.4)),
    as.integer(round(crop_height / 4.8))
  ))

  c(
    list(
      face_x = image_width / 2,
      face_y = crop_top + (crop_height * 0.38),
      face_radius = approx_radius
    ),
    clip_crop_spec(crop_left, crop_top, crop_width, crop_height, image_width, image_height)
  )
}

crop_and_pad_to_canvas <- function(image, crop_spec, canvas_color) {
  clipped <- magick::image_crop(
    image,
    geometry = sprintf(
      "%dx%d+%d+%d",
      crop_spec$clip_width,
      crop_spec$clip_height,
      crop_spec$clip_left,
      crop_spec$clip_top
    )
  )

  canvas <- magick::image_blank(
    width = crop_spec$crop_width,
    height = crop_spec$crop_height,
    color = canvas_color
  )

  magick::image_composite(
    canvas,
    clipped,
    operator = "Over",
    offset = sprintf("+%d+%d", crop_spec$offset_x, crop_spec$offset_y)
  )
}

cache_name_for_relative_path <- function(relative_path) {
  relative_no_ext <- tools::file_path_sans_ext(relative_path)
  relative_no_ext %>%
    gsub("[/\\\\]+", "__", ., perl = TRUE) %>%
    gsub("[^A-Za-z0-9_.-]+", "-", ., perl = TRUE) %>%
    paste0(".png")
}

image_to_gray_matrix <- function(image) {
  gray_image <- magick::image_convert(image, colorspace = "gray")
  gray_data <- magick::image_data(gray_image, channels = "gray")

  matrix(
    as.numeric(gray_data[1, , ]) / 255,
    nrow = dim(gray_data)[2],
    ncol = dim(gray_data)[3]
  )
}

laplacian_blur_score <- function(gray_matrix) {
  if (is.null(gray_matrix) || nrow(gray_matrix) < 3 || ncol(gray_matrix) < 3) {
    return(NA_real_)
  }

  center <- gray_matrix[2:(nrow(gray_matrix) - 1), 2:(ncol(gray_matrix) - 1)]
  laplacian <- 4 * center -
    gray_matrix[1:(nrow(gray_matrix) - 2), 2:(ncol(gray_matrix) - 1)] -
    gray_matrix[3:nrow(gray_matrix), 2:(ncol(gray_matrix) - 1)] -
    gray_matrix[2:(nrow(gray_matrix) - 1), 1:(ncol(gray_matrix) - 2)] -
    gray_matrix[2:(nrow(gray_matrix) - 1), 3:ncol(gray_matrix)]

  stats::sd(as.vector(laplacian), na.rm = TRUE)
}

compute_image_quality_metrics <- function(aligned_image, crop_spec, source_width, source_height, face_count) {
  gray_matrix <- image_to_gray_matrix(aligned_image)
  image_center_x <- source_width / 2
  image_center_y <- source_height / 2
  half_diagonal <- sqrt((source_width / 2)^2 + (source_height / 2)^2)
  face_distance <- sqrt((crop_spec$face_x - image_center_x)^2 + (crop_spec$face_y - image_center_y)^2)

  list(
    blur_score_raw = laplacian_blur_score(gray_matrix),
    face_size_ratio = min(1, (crop_spec$crop_width * crop_spec$crop_height) / (source_width * source_height)),
    center_offset_ratio = min(1, face_distance / half_diagonal),
    luma_mean = mean(gray_matrix, na.rm = TRUE),
    luma_sd = stats::sd(as.vector(gray_matrix), na.rm = TRUE),
    multi_face_flag = isTRUE(face_count > 1L)
  )
}

build_blended_ready <- function(aligned_portrait, aligned_mask = NULL) {
  aligned_portrait <- magick::image_resize(
    aligned_portrait,
    geometry = sprintf("%dx%d!", TARGET_WIDTH, TARGET_HEIGHT)
  )

  if (!is.null(aligned_mask)) {
    aligned_mask <- magick::image_resize(
      aligned_mask,
      geometry = sprintf("%dx%d!", TARGET_WIDTH, TARGET_HEIGHT)
    )
    aligned_mask <- magick::image_blur(aligned_mask, radius = 0, sigma = 4)

    alpha_portrait <- magick::image_composite(
      aligned_portrait,
      aligned_mask,
      operator = "CopyOpacity"
    )

    blended_ready <- magick::image_composite(
      magick::image_blank(width = TARGET_WIDTH, height = TARGET_HEIGHT, color = "white"),
      alpha_portrait,
      operator = "Over"
    )
  } else {
    blended_ready <- magick::image_background(aligned_portrait, color = "white", flatten = TRUE)
  }

  magick::image_convert(blended_ready, colorspace = "sRGB")
}

process_single_portrait <- function(image_row, aligned_cache_dir) {
  issue_rows <- empty_issue_tibble()
  image_out <- image_row
  image_out$aligned_cache_path <- NA_character_

  tmp_input_path <- tempfile(tmpdir = aligned_cache_dir, fileext = ".png")
  tmp_mask_path <- tempfile(tmpdir = aligned_cache_dir, fileext = ".png")

  on.exit({
    unlink(tmp_input_path, force = TRUE)
    unlink(tmp_mask_path, force = TRUE)
  }, add = TRUE)

  tryCatch({
    portrait <- magick::image_read(row_value_or_na(image_row, "image_path"))
    portrait <- magick::image_orient(portrait)
    portrait <- magick::image_convert(portrait, colorspace = "sRGB")
    portrait <- magick::image_normalize(portrait)
    portrait <- magick::image_strip(portrait)

    portrait_info <- magick::image_info(portrait)[1, , drop = FALSE]
    source_width <- as.integer(portrait_info$width[[1]])
    source_height <- as.integer(portrait_info$height[[1]])

    image_out$source_width <- source_width
    image_out$source_height <- source_height

    magick::image_write(portrait, path = tmp_input_path, format = "png")

    detection_result <- tryCatch({
      opencv_image <- opencv::ocv_read(tmp_input_path)
      face_mask <- opencv::ocv_facemask(opencv_image)
      detected_faces <- attr(face_mask, "faces")
      opencv::ocv_write(face_mask, tmp_mask_path)

      list(
        error = NA_character_,
        detected_faces = detected_faces,
        mask_path = if (file.exists(tmp_mask_path)) tmp_mask_path else NA_character_
      )
    }, error = function(e) {
      list(
        error = e$message,
        detected_faces = NULL,
        mask_path = NA_character_
      )
    })

    if (!is.na(detection_result$error)) {
      issue_rows <- dplyr::bind_rows(
        issue_rows,
        build_issue_row(
          image_row,
          "face_detection_error",
          detail = detection_result$error,
          alignment_mode = "portrait_prior_fallback"
        )
      )
    }

    detected_faces <- detection_result$detected_faces
    face_mask_image <- NULL
    crop_spec <- NULL
    face_count <- 0L
    alignment_mode <- "portrait_prior_fallback"

    if (!is.null(detected_faces) && nrow(detected_faces) > 0) {
      detected_faces <- tibble::as_tibble(detected_faces) %>%
        dplyr::arrange(dplyr::desc(.data$radius))

      selected_face <- detected_faces[1, , drop = FALSE]
      face_count <- nrow(detected_faces)
      crop_spec <- compute_crop_spec(source_width, source_height, selected_face)
      alignment_mode <- if (face_count > 1) "multi_face_largest" else "single_face"

      if (face_count > 1) {
        issue_rows <- dplyr::bind_rows(
          issue_rows,
          build_issue_row(
            image_row,
            "multi_face_fallback",
            detail = paste("Selected the largest detected face out of", face_count),
            face_count = face_count,
            alignment_mode = alignment_mode
          )
        )
      }

      if (!is.na(detection_result$mask_path) && file.exists(detection_result$mask_path)) {
        face_mask_image <- magick::image_read(detection_result$mask_path)
        face_mask_image <- magick::image_convert(face_mask_image, colorspace = "gray")
      }
    } else {
      crop_spec <- compute_portrait_prior_crop(source_width, source_height)
      issue_rows <- dplyr::bind_rows(
        issue_rows,
        build_issue_row(
          image_row,
          "portrait_prior_fallback",
          detail = "No face detected. Used portrait-prior fallback crop.",
          face_count = 0L,
          alignment_mode = alignment_mode
        )
      )
    }

    aligned_portrait <- crop_and_pad_to_canvas(portrait, crop_spec, canvas_color = "white")
    aligned_mask <- NULL

    if (!is.null(face_mask_image)) {
      aligned_mask <- crop_and_pad_to_canvas(face_mask_image, crop_spec, canvas_color = "black")
    }

    blended_ready <- build_blended_ready(aligned_portrait, aligned_mask)
    aligned_cache_path <- file.path(
      aligned_cache_dir,
      cache_name_for_relative_path(row_value_or_na(image_row, "relative_path"))
    )

    magick::image_write(blended_ready, path = aligned_cache_path, format = "png")
    quality_metrics <- compute_image_quality_metrics(blended_ready, crop_spec, source_width, source_height, face_count)

    image_out$aligned_cache_path <- normalize_fs_path(aligned_cache_path)
    image_out$processing_status <- "valid"
    image_out$skip_reason <- NA_character_
    image_out$alignment_mode <- alignment_mode
    image_out$face_count <- face_count
    image_out$selected_face_x <- if (face_count > 0L) round(crop_spec$face_x, 2) else NA_real_
    image_out$selected_face_y <- if (face_count > 0L) round(crop_spec$face_y, 2) else NA_real_
    image_out$selected_face_radius <- if (face_count > 0L) crop_spec$face_radius else NA_real_
    image_out$crop_width <- crop_spec$crop_width
    image_out$crop_height <- crop_spec$crop_height
    image_out$aligned_width <- TARGET_WIDTH
    image_out$aligned_height <- TARGET_HEIGHT
    image_out$multi_face_flag <- isTRUE(quality_metrics$multi_face_flag)
    image_out$blur_score_raw <- quality_metrics$blur_score_raw
    image_out$face_size_ratio <- quality_metrics$face_size_ratio
    image_out$center_offset_ratio <- quality_metrics$center_offset_ratio
    image_out$luma_mean <- quality_metrics$luma_mean
    image_out$luma_sd <- quality_metrics$luma_sd

    list(image_row = image_out, issue_rows = issue_rows)
  }, error = function(e) {
    image_out$processing_status <- "skipped"
    image_out$skip_reason <- "read_error"
    image_out$alignment_mode <- "read_error"

    issue_rows <- dplyr::bind_rows(
      issue_rows,
      build_issue_row(
        image_row,
        "read_error",
        detail = e$message,
        alignment_mode = "read_error"
      )
    )

    list(image_row = image_out, issue_rows = issue_rows)
  })
}

safe_percent_rank <- function(x) {
  result <- rep(NA_real_, length(x))
  valid_idx <- !is.na(x)

  if (sum(valid_idx) == 1L) {
    result[valid_idx] <- 1
    return(result)
  }

  if (sum(valid_idx) > 1L) {
    result[valid_idx] <- dplyr::percent_rank(x[valid_idx])
  }

  result
}

derive_low_quality_reason <- function(blur_pct, face_size_pct, center_offset_pct, contrast_pct, exposure_score) {
  penalties <- c(
    bad_exposure = ifelse(is.na(exposure_score), 1, 1 - exposure_score),
    low_blur = ifelse(is.na(blur_pct), 1, 1 - blur_pct),
    low_contrast = ifelse(is.na(contrast_pct), 1, 1 - contrast_pct),
    off_center = ifelse(is.na(center_offset_pct), 1, center_offset_pct),
    small_face = ifelse(is.na(face_size_pct), 1, 1 - face_size_pct)
  )

  names(penalties)[order(-penalties, names(penalties))][1]
}

compute_quality_scores <- function(image_manifest) {
  image_manifest <- image_manifest %>%
    dplyr::mutate(
      blur_pct = safe_percent_rank(.data$blur_score_raw),
      face_size_pct = safe_percent_rank(.data$face_size_ratio),
      contrast_pct = safe_percent_rank(.data$luma_sd),
      center_offset_pct = dplyr::if_else(
        !is.na(.data$center_offset_ratio),
        pmin(pmax(.data$center_offset_ratio, 0), 1),
        NA_real_
      ),
      exposure_score = dplyr::if_else(
        !is.na(.data$luma_mean),
        pmax(0, 1 - abs(.data$luma_mean - 0.5) / 0.5),
        NA_real_
      ),
      quality_score = dplyr::if_else(
        .data$processing_status == "valid",
        0.35 * .data$blur_pct +
          0.25 * .data$face_size_pct +
          0.15 * (1 - .data$center_offset_pct) +
          0.15 * .data$contrast_pct +
          0.10 * .data$exposure_score,
        NA_real_
      ),
      quality_band = dplyr::case_when(
        is.na(.data$quality_score) ~ NA_character_,
        .data$quality_score >= 0.70 ~ "high",
        .data$quality_score >= QUALITY_THRESHOLD ~ "medium",
        TRUE ~ "low"
      ),
      eligible_all = .data$processing_status == "valid"
    )

  low_quality_reasons <- purrr::pmap_chr(
    list(
      image_manifest$blur_pct,
      image_manifest$face_size_pct,
      image_manifest$center_offset_pct,
      image_manifest$contrast_pct,
      image_manifest$exposure_score
    ),
    derive_low_quality_reason
  )

  image_manifest %>%
    dplyr::mutate(
      quality_exclusion_reason = dplyr::case_when(
        .data$processing_status != "valid" ~ dplyr::coalesce(.data$skip_reason, "read_error"),
        .data$face_count > 1 ~ "multi_face",
        .data$face_count == 0 ~ "no_single_face",
        is.na(.data$quality_score) ~ "missing_quality_score",
        .data$quality_score < QUALITY_THRESHOLD ~ low_quality_reasons,
        TRUE ~ NA_character_
      ),
      eligible_filtered = .data$eligible_all & .data$face_count == 1 & .data$quality_score >= QUALITY_THRESHOLD
    )
}

group_mode_fields <- function(group_mode) {
  switch(
    group_mode,
    all = character(),
    sex = c("sex_assigned"),
    region = c("region_slug"),
    region_sex = c("region_slug", "sex_assigned"),
    affiliation = c("affiliation_slug"),
    region_affiliation = c("region_slug", "affiliation_slug"),
    stop("Unsupported grouping mode: ", group_mode, call. = FALSE)
  )
}

blend_root <- function(blend_mode) {
  if (identical(blend_mode, "filtered")) {
    return(FILTERED_ROOT)
  }

  if (identical(blend_mode, "all_faces")) {
    return(ALL_FACES_ROOT)
  }

  stop("Unsupported blend mode: ", blend_mode, call. = FALSE)
}

build_group_paths <- function(blend_mode, group_mode, region_slug = NA_character_, affiliation_slug = NA_character_, sex_assigned = NA_character_) {
  root_dir <- blend_root(blend_mode)

  if (identical(group_mode, "all")) {
    return(list(
      output_path = file.path(root_dir, "all.jpg"),
      qc_path = file.path(root_dir, "qc", "all.jpg")
    ))
  }

  if (identical(group_mode, "sex")) {
    return(list(
      output_path = file.path(root_dir, "by_sex", paste0(sex_assigned, ".jpg")),
      qc_path = file.path(root_dir, "qc", "by_sex", paste0(sex_assigned, ".jpg"))
    ))
  }

  if (identical(group_mode, "region")) {
    return(list(
      output_path = file.path(root_dir, "by_region", paste0(region_slug, ".jpg")),
      qc_path = file.path(root_dir, "qc", "by_region", paste0(region_slug, ".jpg"))
    ))
  }

  if (identical(group_mode, "region_sex")) {
    return(list(
      output_path = file.path(root_dir, "by_region_sex", region_slug, paste0(sex_assigned, ".jpg")),
      qc_path = file.path(root_dir, "qc", "by_region_sex", region_slug, paste0(sex_assigned, ".jpg"))
    ))
  }

  if (identical(group_mode, "affiliation")) {
    return(list(
      output_path = file.path(root_dir, "by_affiliation", paste0(affiliation_slug, ".jpg")),
      qc_path = file.path(root_dir, "qc", "by_affiliation", paste0(affiliation_slug, ".jpg"))
    ))
  }

  list(
    output_path = file.path(
      root_dir,
      "by_region_affiliation",
      region_slug,
      paste0(affiliation_slug, ".jpg")
    ),
    qc_path = file.path(
      root_dir,
      "qc",
      "by_region_affiliation",
      region_slug,
      paste0(affiliation_slug, ".jpg")
    )
  )
}

build_group_record <- function(group_rows, blend_mode, group_mode) {
  first_row <- group_rows[1, , drop = FALSE]
  region_slug <- row_value_or_na(first_row, "region_slug")
  affiliation_slug <- row_value_or_na(first_row, "affiliation_slug")
  region_label <- row_value_or_na(first_row, "region_label")
  affiliation_label <- row_value_or_na(first_row, "affiliation_label")
  sex_assigned <- row_value_or_na(first_row, "sex_assigned")
  paths <- build_group_paths(blend_mode, group_mode, region_slug, affiliation_slug, sex_assigned)

  group_key <- switch(
    group_mode,
    all = "all",
    sex = sex_assigned,
    region = region_slug,
    region_sex = paste(region_slug, sex_assigned, sep = "__"),
    affiliation = affiliation_slug,
    region_affiliation = paste(region_slug, affiliation_slug, sep = "__")
  )

  group_label <- switch(
    group_mode,
    all = "All Candidates",
    sex = sex_display_label(sex_assigned),
    region = region_label,
    region_sex = paste(region_label, sex_display_label(sex_assigned), sep = " / "),
    affiliation = affiliation_label,
    region_affiliation = paste(region_label, affiliation_label, sep = " / ")
  )

  tibble::tibble(
    blend_mode = blend_mode,
    group_mode = group_mode,
    group_key = group_key,
    group_label = group_label,
    sex_assigned = sex_assigned,
    region_slug = region_slug,
    affiliation_slug = affiliation_slug,
    output_path = normalize_fs_path(paths$output_path),
    qc_path = normalize_fs_path(paths$qc_path)
  )
}

reset_generated_output_dirs <- function() {
  targets <- c(
    FILTERED_ROOT,
    ALL_FACES_ROOT,
    MANIFEST_ROOT,
    # Remove compatibility folders from earlier runs and do not recreate them.
    file.path(OUTPUT_ROOT, "by_region"),
    file.path(OUTPUT_ROOT, "by_affiliation"),
    file.path(OUTPUT_ROOT, "by_region_affiliation"),
    file.path(OUTPUT_ROOT, "qc")
  )

  for (target in targets) {
    if (dir.exists(target) || file.exists(target)) {
      unlink(target, recursive = TRUE, force = TRUE)
    }
  }

  invisible(targets)
}

ensure_average_face_dirs <- function() {
  dirs <- c(
    OUTPUT_ROOT,
    FILTERED_ROOT,
    ALL_FACES_ROOT,
    MANIFEST_ROOT,
    file.path(FILTERED_ROOT, "by_region"),
    file.path(FILTERED_ROOT, "by_region_sex"),
    file.path(FILTERED_ROOT, "by_affiliation"),
    file.path(FILTERED_ROOT, "by_region_affiliation"),
    file.path(FILTERED_ROOT, "by_sex"),
    file.path(FILTERED_ROOT, "qc"),
    file.path(FILTERED_ROOT, "qc", "by_region"),
    file.path(FILTERED_ROOT, "qc", "by_region_sex"),
    file.path(FILTERED_ROOT, "qc", "by_affiliation"),
    file.path(FILTERED_ROOT, "qc", "by_region_affiliation"),
    file.path(FILTERED_ROOT, "qc", "by_sex"),
    file.path(ALL_FACES_ROOT, "by_region"),
    file.path(ALL_FACES_ROOT, "by_region_sex"),
    file.path(ALL_FACES_ROOT, "by_affiliation"),
    file.path(ALL_FACES_ROOT, "by_region_affiliation"),
    file.path(ALL_FACES_ROOT, "by_sex"),
    file.path(ALL_FACES_ROOT, "qc"),
    file.path(ALL_FACES_ROOT, "qc", "by_region"),
    file.path(ALL_FACES_ROOT, "qc", "by_region_sex"),
    file.path(ALL_FACES_ROOT, "qc", "by_affiliation"),
    file.path(ALL_FACES_ROOT, "qc", "by_region_affiliation"),
    file.path(ALL_FACES_ROOT, "qc", "by_sex"),
    file.path("data", "reference"),
    file.path("data", "manual")
  )

  for (dir_path in dirs) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }

  invisible(dirs)
}

qc_tile_value <- function(n_images) {
  paste0(max(1L, ceiling(sqrt(n_images))), "x")
}

select_preview_paths <- function(paths, max_images = MAX_QC_PREVIEW_IMAGES) {
  paths <- normalize_fs_path(paths)

  if (length(paths) <= max_images) {
    return(paths)
  }

  selected_idx <- unique(round(seq(1, length(paths), length.out = max_images)))
  paths[selected_idx]
}

build_qc_montage <- function(aligned_paths, composite_image, qc_path) {
  preview_paths <- select_preview_paths(aligned_paths)
  preview_stack <- magick::image_read(preview_paths)
  preview_stack <- magick::image_resize(preview_stack, geometry = "120x170!")

  composite_preview <- magick::image_resize(composite_image, geometry = "120x170!")
  montage_input <- magick::image_join(preview_stack, composite_preview)

  montage <- magick::image_montage(
    montage_input,
    geometry = "120x170+4+4",
    tile = qc_tile_value(length(preview_paths) + 1L),
    gravity = "Center",
    bg = "white",
    shadow = FALSE
  )

  dir.create(dirname(qc_path), recursive = TRUE, showWarnings = FALSE)
  magick::image_write(montage, path = qc_path, format = "jpg", quality = 92)

  normalize_fs_path(qc_path)
}

render_group_average <- function(group_rows, blend_mode, group_mode) {
  group_meta <- build_group_record(group_rows, blend_mode, group_mode)
  eligibility_col <- if (identical(blend_mode, "filtered")) "eligible_filtered" else "eligible_all"

  discovered_count <- nrow(group_rows)
  eligible_rows <- group_rows %>%
    dplyr::filter(.data[[eligibility_col]]) %>%
    dplyr::arrange(.data$id_candidato, .data$file_name)

  if (!is.na(MAX_IMAGES_PER_GROUP)) {
    eligible_rows <- dplyr::slice_head(eligible_rows, n = MAX_IMAGES_PER_GROUP)
  }

  eligible_count <- nrow(eligible_rows)
  excluded_count <- discovered_count - eligible_count

  if (eligible_count < MIN_VALID_IMAGES) {
    return(dplyr::bind_cols(
      group_meta,
      tibble::tibble(
        discovered_count = discovered_count,
        eligible_count = eligible_count,
        excluded_count = excluded_count,
        rendered = FALSE,
        skip_reason = "insufficient_n",
        qc_output_path = NA_character_
      )
    ))
  }

  aligned_paths <- normalize_fs_path(eligible_rows$aligned_cache_path)
  aligned_stack <- magick::image_read(aligned_paths)
  average_face <- magick::image_average(aligned_stack)
  average_face <- magick::image_background(average_face, color = "white", flatten = TRUE)

  output_path <- row_value_or_na(group_meta, "output_path")
  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  magick::image_write(average_face, path = output_path, format = "jpg", quality = 95)

  qc_output_path <- NA_character_

  if (isTRUE(SAVE_QC_MONTAGES)) {
    qc_output_path <- build_qc_montage(
      aligned_paths,
      average_face,
      row_value_or_na(group_meta, "qc_path")
    )
  }

  dplyr::bind_cols(
    group_meta,
    tibble::tibble(
      discovered_count = discovered_count,
      eligible_count = eligible_count,
      excluded_count = excluded_count,
      rendered = TRUE,
      skip_reason = NA_character_,
      qc_output_path = qc_output_path
    )
  )
}

split_group_rows <- function(image_manifest, group_mode) {
  grouping_fields <- group_mode_fields(group_mode)
  ordered_manifest <- image_manifest %>%
    dplyr::arrange(
      .data$region_slug,
      .data$affiliation_slug,
      .data$sex_assigned,
      .data$id_candidato,
      .data$file_name
    )

  if (length(grouping_fields) == 0) {
    return(list(ordered_manifest))
  }

  ordered_manifest %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(grouping_fields))) %>%
    dplyr::group_split(.keep = TRUE)
}

build_group_manifest <- function(image_manifest) {
  group_manifest_rows <- list()
  row_index <- 1L

  # Render every requested grouping for both blend modes. The manifest becomes
  # the single place that explains what was rendered, skipped, or filtered out.
  for (blend_mode in BLEND_MODES) {
    for (group_mode in GROUP_MODES) {
      grouped <- split_group_rows(image_manifest, group_mode)

      for (group_rows in grouped) {
        if (nrow(group_rows) == 0) {
          next
        }

        group_manifest_rows[[row_index]] <- render_group_average(group_rows, blend_mode, group_mode)
        row_index <- row_index + 1L
      }
    }
  }

  if (length(group_manifest_rows) == 0) {
    return(tibble::tibble(
      blend_mode = character(),
      group_mode = character(),
      group_key = character(),
      group_label = character(),
      sex_assigned = character(),
      region_slug = character(),
      affiliation_slug = character(),
      output_path = character(),
      qc_path = character(),
      discovered_count = character(),
      eligible_count = character(),
      excluded_count = character(),
      rendered = character(),
      skip_reason = character(),
      qc_output_path = character()
    ))
  }

  dplyr::bind_rows(group_manifest_rows)
}

format_example_values <- function(df, eligibility_col, retained = TRUE, max_examples = 3L) {
  example_rows <- if (retained) {
    df %>%
      dplyr::filter(.data[[eligibility_col]]) %>%
      dplyr::arrange(.data$id_candidato, .data$file_name)
  } else {
    df %>%
      dplyr::filter(!.data[[eligibility_col]]) %>%
      dplyr::arrange(.data$id_candidato, .data$file_name)
  }

  if (nrow(example_rows) == 0) {
    return(NA_character_)
  }

  examples <- example_rows %>%
    dplyr::transmute(example = paste(.data$id_candidato, .data$nombre, sep = "|")) %>%
    dplyr::distinct(.data$example) %>%
    dplyr::slice_head(n = max_examples) %>%
    dplyr::pull(.data$example)

  if (length(examples) == 0) {
    return(NA_character_)
  }

  paste(examples, collapse = "; ")
}

top_exclusion_reason <- function(df) {
  excluded_df <- df %>%
    dplyr::filter(!.data$eligible_filtered)

  if (nrow(excluded_df) == 0) {
    return(NA_character_)
  }

  excluded_df %>%
    dplyr::mutate(
      quality_exclusion_reason = dplyr::coalesce(.data$quality_exclusion_reason, "unknown")
    ) %>%
    dplyr::count(.data$quality_exclusion_reason, sort = TRUE, name = "n") %>%
    dplyr::arrange(dplyr::desc(.data$n), .data$quality_exclusion_reason) %>%
    dplyr::slice(1) %>%
    dplyr::pull(.data$quality_exclusion_reason)
}

build_exclusion_summary <- function(image_manifest, group_type) {
  grouping_fields <- if (identical(group_type, "party")) {
    c("affiliation_slug")
  } else {
    c("region_slug")
  }

  grouped_rows <- image_manifest %>%
    dplyr::arrange(
      .data$region_slug,
      .data$affiliation_slug,
      .data$id_candidato,
      .data$file_name
    ) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(grouping_fields))) %>%
    dplyr::group_split(.keep = TRUE)

  summary_rows <- purrr::map_dfr(grouped_rows, function(group_rows) {
    first_row <- group_rows[1, , drop = FALSE]
    discovered_count <- nrow(group_rows)
    retained_filtered <- sum(group_rows$eligible_filtered, na.rm = TRUE)
    excluded_filtered <- discovered_count - retained_filtered
    retained_all <- sum(group_rows$eligible_all, na.rm = TRUE)
    excluded_all <- discovered_count - retained_all

    tibble::tibble(
      group_type = group_type,
      group_slug = if (identical(group_type, "party")) {
        row_value_or_na(first_row, "affiliation_slug")
      } else {
        row_value_or_na(first_row, "region_slug")
      },
      group_label = if (identical(group_type, "party")) {
        coalesce_chr(row_value_or_na(first_row, "partido_politico"), row_value_or_na(first_row, "affiliation_label"))
      } else {
        coalesce_chr(row_value_or_na(first_row, "distrito_electoral"), row_value_or_na(first_row, "region_label"))
      },
      discovered_count = discovered_count,
      retained_filtered = retained_filtered,
      excluded_filtered = excluded_filtered,
      retained_all = retained_all,
      excluded_all = excluded_all,
      exclusion_rate_filtered = if (discovered_count > 0) excluded_filtered / discovered_count else NA_real_,
      top_exclusion_reason = top_exclusion_reason(group_rows),
      retained_examples = format_example_values(group_rows, "eligible_filtered", retained = TRUE),
      excluded_examples = format_example_values(group_rows, "eligible_filtered", retained = FALSE)
    )
  })

  summary_rows %>%
    dplyr::arrange(.data$group_label, .data$group_slug)
}

build_sex_review_queue <- function(candidate_assignments, image_manifest) {
  preview_paths <- image_manifest %>%
    dplyr::arrange(.data$id_candidato, .data$file_name) %>%
    dplyr::group_by(.data$id_candidato) %>%
    dplyr::summarise(
      preview_image_path = dplyr::first(.data$image_path),
      .groups = "drop"
    )

  candidate_assignments %>%
    dplyr::filter(.data$needs_manual_review) %>%
    dplyr::left_join(preview_paths, by = "id_candidato") %>%
    dplyr::arrange(.data$sex_confidence, .data$nombre)
}

# ============================
# ==========  SETUP ==========
# ============================

create_env()
reset_generated_output_dirs()
ensure_average_face_dirs()

aligned_cache_dir <- file.path(
  tempdir(),
  paste0("jne_average_faces_", format(Sys.time(), "%Y%m%d_%H%M%S"))
)
dir.create(aligned_cache_dir, recursive = TRUE, showWarnings = FALSE)

candidate_metadata <- load_candidate_metadata(CANDIDATES_CSV)
first_name_lookup <- load_first_name_lookup(FIRST_NAME_LOOKUP_CSV)
sex_overrides <- load_sex_overrides(SEX_OVERRIDE_CSV)
candidate_assignments <- assign_candidate_sex(candidate_metadata, first_name_lookup, sex_overrides)
image_manifest <- discover_portrait_manifest(INPUT_IMAGE_ROOT, candidate_assignments) %>%
  fill_missing_portrait_sex_assignments(first_name_lookup)

# First evaluate portraits one by one, then build composite outputs from the
# cleaned manifest instead of mixing validation and rendering together.
log_message(paste("Portrait files discovered:", nrow(image_manifest)))
log_message(paste("Candidate metadata rows loaded:", nrow(candidate_metadata)))
log_message(paste("First-name lookup rows loaded:", nrow(first_name_lookup)))
log_message(paste("Sex override rows loaded:", nrow(sex_overrides)))


# ====================================
# ==========  IMAGE QC LOOP ==========
# ====================================

processed_rows <- vector("list", nrow(image_manifest))
issue_rows <- list()
issue_index <- 1L

for (row_idx in seq_len(nrow(image_manifest))) {
  image_row <- image_manifest[row_idx, , drop = FALSE]

  if (row_idx %% 25L == 1L || row_idx == nrow(image_manifest)) {
    log_message(paste0(
      "Processing portrait ", row_idx, "/", nrow(image_manifest), ": ",
      row_value_or_na(image_row, "relative_path")
    ))
  }

  if (isTRUE(image_row$metadata_conflict[[1]])) {
    issue_rows[[issue_index]] <- build_issue_row(
      image_row,
      "metadata_mismatch",
      detail = metadata_conflict_detail(image_row),
      alignment_mode = "metadata_review"
    )
    issue_index <- issue_index + 1L
  }

  processed <- process_single_portrait(image_row, aligned_cache_dir)
  processed_rows[[row_idx]] <- processed$image_row

  if (!is.null(processed$issue_rows) && nrow(processed$issue_rows) > 0) {
    issue_rows[[issue_index]] <- processed$issue_rows
    issue_index <- issue_index + 1L
  }
}

processed_manifest <- dplyr::bind_rows(processed_rows) %>%
  dplyr::arrange(.data$region_slug, .data$affiliation_slug, .data$id_candidato, .data$file_name) %>%
  compute_quality_scores()

skipped_images <- if (length(issue_rows) > 0) {
  dplyr::bind_rows(issue_rows)
} else {
  empty_issue_tibble()
}


# ======================================
# ==========  GROUP AVERAGES ============
# ======================================

group_manifest <- build_group_manifest(processed_manifest) %>%
  dplyr::arrange(.data$blend_mode, .data$group_mode, .data$group_key)


# =======================================
# ==========  EXTRA MANIFESTS ============
# =======================================

sex_assignment_manifest <- processed_manifest %>%
  dplyr::distinct(
    .data$id_candidato,
    .data$nombre,
    .data$sexo_raw,
    .data$sex_assigned,
    .data$sex_source,
    .data$sex_confidence,
    .data$sex_note
  ) %>%
  dplyr::left_join(
    candidate_assignments %>%
      dplyr::select("id_candidato", "matched_name_key", "needs_manual_review"),
    by = "id_candidato"
  ) %>%
  dplyr::mutate(
    needs_manual_review = dplyr::coalesce(
      .data$needs_manual_review,
      !.data$sex_source %in% c("official_declared", "manual_visual_override", "first_name_lookup", "first_name_lookup_compound")
    )
  ) %>%
  dplyr::arrange(.data$nombre, .data$id_candidato)

sex_review_queue <- build_sex_review_queue(sex_assignment_manifest, processed_manifest)

exclusion_summary_by_party <- build_exclusion_summary(processed_manifest, "party")
exclusion_summary_by_department <- build_exclusion_summary(processed_manifest, "department")


# =======================================
# ==========  WRITE MANIFESTS ============
# =======================================

image_manifest_output <- processed_manifest %>%
  dplyr::select(
    "id_candidato",
    "nombre",
    "sexo_raw",
    "sex_assigned",
    "sex_source",
    "sex_confidence",
    "sex_note",
    "image_path",
    "relative_path",
    "file_name",
    "region_slug",
    "affiliation_slug",
    "region_label",
    "affiliation_label",
    "distrito_electoral",
    "partido_politico",
    "csv_region_slug",
    "csv_affiliation_slug",
    "metadata_region_conflict",
    "metadata_affiliation_conflict",
    "metadata_conflict",
    "processing_status",
    "skip_reason",
    "alignment_mode",
    "face_count",
    "multi_face_flag",
    "selected_face_x",
    "selected_face_y",
    "selected_face_radius",
    "source_width",
    "source_height",
    "crop_width",
    "crop_height",
    "aligned_width",
    "aligned_height",
    "blur_score_raw",
    "blur_pct",
    "face_size_ratio",
    "face_size_pct",
    "center_offset_ratio",
    "center_offset_pct",
    "luma_mean",
    "luma_sd",
    "contrast_pct",
    "exposure_score",
    "quality_score",
    "quality_band",
    "quality_exclusion_reason",
    "eligible_filtered",
    "eligible_all",
    "aligned_cache_path"
  )

image_manifest_path <- file.path(MANIFEST_ROOT, "image_manifest.csv")
group_manifest_path <- file.path(MANIFEST_ROOT, "group_manifest.csv")
skipped_images_path <- file.path(MANIFEST_ROOT, "skipped_images.csv")
sex_assignment_manifest_path <- file.path(MANIFEST_ROOT, "sex_assignment_manifest.csv")
sex_review_queue_path <- file.path(MANIFEST_ROOT, "sex_review_queue.csv")
exclusion_by_party_path <- file.path(MANIFEST_ROOT, "exclusion_summary_by_party.csv")
exclusion_by_department_path <- file.path(MANIFEST_ROOT, "exclusion_summary_by_department.csv")

write_csv_chr(image_manifest_output, image_manifest_path)
write_csv_chr(group_manifest, group_manifest_path)
write_csv_chr(skipped_images, skipped_images_path)
write_csv_chr(sex_assignment_manifest, sex_assignment_manifest_path)
write_csv_chr(sex_review_queue, sex_review_queue_path)
write_csv_chr(exclusion_summary_by_party, exclusion_by_party_path)
write_csv_chr(exclusion_summary_by_department, exclusion_by_department_path)


# ======================================
# ==========  RUNTIME SUMMARY ===========
# ======================================

filtered_retained <- processed_manifest %>%
  dplyr::filter(.data$eligible_filtered) %>%
  nrow()

all_faces_retained <- processed_manifest %>%
  dplyr::filter(.data$eligible_all) %>%
  nrow()

rendered_groups <- group_manifest %>%
  dplyr::filter(.data$rendered) %>%
  nrow()

skipped_groups <- group_manifest %>%
  dplyr::filter(!.data$rendered) %>%
  nrow()

log_message(paste("Readable portraits retained in all_faces:", all_faces_retained))
log_message(paste("Conservative filtered portraits retained:", filtered_retained))
log_message(paste("Portrait issue rows:", nrow(skipped_images)))
log_message(paste("Rendered average-face groups:", rendered_groups))
log_message(paste("Skipped groups:", skipped_groups))
log_message(paste("Image manifest written to:", normalize_fs_path(image_manifest_path)))
log_message(paste("Group manifest written to:", normalize_fs_path(group_manifest_path)))
log_message(paste("Sex assignment manifest written to:", normalize_fs_path(sex_assignment_manifest_path)))
log_message(paste("Sex review queue written to:", normalize_fs_path(sex_review_queue_path)))
log_message(paste("Party exclusion summary written to:", normalize_fs_path(exclusion_by_party_path)))
log_message(paste("Department exclusion summary written to:", normalize_fs_path(exclusion_by_department_path)))
log_message("Average candidate face generation complete.")
