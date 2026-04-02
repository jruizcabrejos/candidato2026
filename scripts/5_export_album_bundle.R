######################################################################################
# Project: Scrapper and processing of Peru's 2026 election candidate data
# Script: 5_export_album_bundle.R - Export album handoff bundle from processed data
# Author: Jorge Ruiz-Cabrejos and Inca Slop
# Created: 2026-03-26
# Last Updated: 2026-04-02
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
  "jsonlite",
  "magick"
)

CANDIDATES_CSV <- file.path("output", "candidatos.csv")
SENTENCES_CSV <- file.path("output", "relacion_sentencias.csv")
WORK_CSV <- file.path("output", "experiencia_laboral.csv")
IMAGE_ROOT <- file.path("output", "images")
QC_MANIFEST_CSV <- file.path("output", "average_faces", "manifests", "image_manifest.csv")

EXPORT_ROOT_JPG <- file.path("output", "album_export")
EXPORT_ROOT_WEBP <- file.path("output", "album_export_webp")

WEBP_QUALITY <- suppressWarnings(
  as.integer(Sys.getenv("JNE_EXPORT_WEBP_QUALITY", unset = "90"))
)

if (length(WEBP_QUALITY) == 0L || is.na(WEBP_QUALITY) || WEBP_QUALITY < 1L || WEBP_QUALITY > 100L) {
  WEBP_QUALITY <- 90L
}

WEBP_MAX_BYTES <- 15L * 1024L
WEBP_MIN_QUALITY <- 1L
PORTRAIT_TARGET_WIDTH <- 427L
PORTRAIT_TARGET_HEIGHT <- 602L
PORTRAIT_UPPER_CENTER_BIAS <- 0.15

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
  known_type_tokens <- c("diputado", "senador", "presidente", "vicepresidente")
  id_length <- if (length(parts) >= 3 && parts[3] %in% known_type_tokens) 3L else if (length(parts) >= 2) 2L else NA_integer_

  if (is.na(id_length)) {
    return(NA_character_)
  }

  paste(parts[seq_len(id_length)], collapse = "_")
}

type_export_code <- function(type_value) {
  type_key <- clean_name_es(type_value)

  if (identical(type_key, "presidente")) {
    return("pre")
  }

  if (identical(type_key, "vicepresidente")) {
    return("vp")
  }

  if (identical(type_key, "senador")) {
    return("sen")
  }

  if (identical(type_key, "diputado")) {
    return("dip")
  }

  "cand"
}

build_export_id <- function(id_candidato, type_value = NA_character_) {
  raw_id <- paste0("cand_", value_or_na(id_candidato))

  if (!is.na(raw_id) && nchar(raw_id) <= 32L) {
    return(raw_id)
  }

  parts <- strsplit(coalesce_chr(id_candidato), "_", fixed = TRUE)[[1]]

  if (length(parts) >= 2) {
    return(paste("cand", parts[1], parts[2], type_export_code(type_value), sep = "_"))
  }

  raw_id
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

build_bundle_paths <- function(export_root) {
  export_manifest_root <- file.path(export_root, "manifests")

  list(
    export_root = export_root,
    export_image_root = file.path(export_root, "images", "candidates"),
    export_manifest_root = export_manifest_root,
    export_json_path = file.path(export_root, "candidates.json"),
    export_manifest_path = file.path(export_manifest_root, "export_manifest.csv"),
    party_manifest_path = file.path(export_manifest_root, "party_manifest.csv"),
    import_guide_path = file.path(export_root, "import_guide.md")
  )
}

rebuild_export_dirs <- function(export_root) {
  export_root_norm <- normalize_export_path(export_root)

  # Guard against accidental deletes outside the expected export folder before
  # rebuilding the handoff bundle from scratch.
  if (!grepl("/output/album_export(?:_webp)?$", export_root_norm)) {
    abort_export(paste("Refusing to rebuild unexpected export directory:", export_root_norm))
  }

  if (dir.exists(export_root)) {
    unlink(export_root, recursive = TRUE, force = TRUE)
  }

  bundle_paths <- build_bundle_paths(export_root)

  dir.create(bundle_paths$export_image_root, recursive = TRUE, showWarnings = FALSE)
  dir.create(bundle_paths$export_manifest_root, recursive = TRUE, showWarnings = FALSE)

  invisible(bundle_paths)
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

inspect_source_portrait <- function(path) {
  path <- normalize_export_path(path)

  if (!file.exists(path)) {
    return(list(valid = FALSE, reason = "missing_file", detail = NA_character_))
  }

  file_info <- suppressWarnings(file.info(path))
  file_size <- as.numeric(file_info$size[[1]])

  if (is.na(file_size) || file_size <= 0) {
    return(list(valid = FALSE, reason = "empty_file", detail = NA_character_))
  }

  tryCatch({
    suppressWarnings(magick::image_read(path))
    list(valid = TRUE, reason = NA_character_, detail = NA_character_)
  }, error = function(e) {
    list(
      valid = FALSE,
      reason = "unreadable_image",
      detail = trimws(gsub("[\r\n]+", " ", conditionMessage(e)))
    )
  })
}

validate_export_portraits <- function(export_df, max_examples = 5L) {
  portrait_checks <- purrr::map(export_df$source_image_abs_path, inspect_source_portrait)
  invalid_flags <- purrr::map_lgl(portrait_checks, function(x) !isTRUE(x$valid))

  if (!any(invalid_flags)) {
    return(invisible(TRUE))
  }

  invalid_checks <- portrait_checks[invalid_flags]
  invalid_df <- export_df[invalid_flags, , drop = FALSE] %>%
    dplyr::mutate(
      validation_reason = purrr::map_chr(invalid_checks, function(x) value_or_na(x$reason)),
      validation_detail = purrr::map_chr(invalid_checks, function(x) value_or_na(x$detail)),
      source_image_path = portable_rel_path("output", "images", .data$source_image_relative_path)
    )

  invalid_preview <- invalid_df %>%
    dplyr::slice_head(n = max_examples) %>%
    dplyr::transmute(
      preview = ifelse(
        is.na(.data$validation_detail),
        paste0(.data$source_image_path, " [", .data$validation_reason, "]"),
        paste0(.data$source_image_path, " [", .data$validation_reason, ": ", .data$validation_detail, "]")
      )
    ) %>%
    dplyr::pull(.data$preview)

  abort_export(
    paste(
      paste0("Invalid portrait source files were found under output/images (", nrow(invalid_df), ")."),
      paste0("Examples: ", paste(invalid_preview, collapse = " | ")),
      "Repair or redownload the broken portraits, then rerun script 5.",
      sep = " "
    )
  )
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

sanitize_webp_quality <- function(quality) {
  quality <- suppressWarnings(as.integer(quality))

  if (length(quality) == 0L || is.na(quality)) {
    return(WEBP_QUALITY)
  }

  max(WEBP_MIN_QUALITY, min(100L, quality[[1]]))
}

file_size_bytes <- function(path) {
  file_info <- suppressWarnings(file.info(path))

  if (is.null(file_info) || nrow(file_info) == 0L) {
    return(NA_real_)
  }

  as.numeric(file_info$size[[1]])
}

compute_portrait_prior_crop <- function(
  image_width,
  image_height,
  target_width = PORTRAIT_TARGET_WIDTH,
  target_height = PORTRAIT_TARGET_HEIGHT
) {
  target_ratio <- target_width / target_height
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
  crop_top <- as.integer(floor((image_height - crop_height) * PORTRAIT_UPPER_CENTER_BIAS))
  crop_top <- max(0L, min(crop_top, image_height - crop_height))

  list(
    crop_width = crop_width,
    crop_height = crop_height,
    crop_left = crop_left,
    crop_top = crop_top
  )
}

normalize_webp_portrait <- function(source_path) {
  image <- magick::image_read(source_path)
  image <- magick::image_orient(image)

  image_info <- magick::image_info(image)

  if (nrow(image_info) == 0L || is.na(image_info$width[[1]]) || is.na(image_info$height[[1]])) {
    abort_export(paste("Could not inspect portrait dimensions for:", source_path))
  }

  crop_spec <- compute_portrait_prior_crop(
    image_width = image_info$width[[1]],
    image_height = image_info$height[[1]]
  )

  image <- magick::image_crop(
    image,
    geometry = sprintf(
      "%dx%d+%d+%d",
      crop_spec$crop_width,
      crop_spec$crop_height,
      crop_spec$crop_left,
      crop_spec$crop_top
    )
  )

  magick::image_strip(image)
}

image_dimensions <- function(image) {
  image_info <- magick::image_info(image)

  if (nrow(image_info) == 0L || is.na(image_info$width[[1]]) || is.na(image_info$height[[1]])) {
    abort_export("Could not inspect in-memory image dimensions.")
  }

  list(
    width = as.integer(image_info$width[[1]]),
    height = as.integer(image_info$height[[1]])
  )
}

resize_webp_fallback_image <- function(image, scale) {
  scale <- suppressWarnings(as.numeric(scale))

  if (length(scale) == 0L || is.na(scale)) {
    return(image)
  }

  scale <- max(0.05, min(1, scale[[1]]))

  if (scale >= 0.999) {
    return(image)
  }

  dims <- image_dimensions(image)
  resized_width <- max(1L, as.integer(round(dims$width * scale)))
  resized_height <- max(1L, as.integer(round(dims$height * scale)))

  magick::image_resize(
    image,
    geometry = sprintf("%dx%d!", resized_width, resized_height)
  )
}

write_webp_candidate <- function(image, path, quality) {
  if (file.exists(path)) {
    unlink(path, force = TRUE)
  }

  magick::image_write(
    image,
    path = path,
    format = "webp",
    quality = sanitize_webp_quality(quality)
  )

  file_size_bytes(path)
}

fit_webp_candidate <- function(image, quality = WEBP_QUALITY, max_bytes = WEBP_MAX_BYTES, temp_path, best_path) {
  target_quality <- sanitize_webp_quality(quality)
  dims <- image_dimensions(image)

  if (file.exists(best_path)) {
    unlink(best_path, force = TRUE)
  }

  best_quality <- NA_integer_
  best_size <- NA_real_

  persist_candidate <- function(candidate_quality, candidate_size) {
    file.copy(temp_path, best_path, overwrite = TRUE)
    best_quality <<- candidate_quality
    best_size <<- candidate_size
  }

  initial_size <- write_webp_candidate(image, temp_path, target_quality)

  if (!is.na(initial_size) && initial_size <= max_bytes) {
    persist_candidate(target_quality, initial_size)
  } else {
    low <- WEBP_MIN_QUALITY
    high <- max(WEBP_MIN_QUALITY, target_quality - 1L)

    while (low <= high) {
      candidate_quality <- as.integer(floor((low + high) / 2))
      candidate_size <- write_webp_candidate(image, temp_path, candidate_quality)

      if (is.na(candidate_size)) {
        break
      }

      if (candidate_size <= max_bytes) {
        persist_candidate(candidate_quality, candidate_size)
        low <- candidate_quality + 1L
      } else {
        high <- candidate_quality - 1L
      }
    }
  }

  if (is.na(best_quality)) {
    fallback_size <- write_webp_candidate(image, temp_path, WEBP_MIN_QUALITY)

    return(list(
      within_limit = isTRUE(!is.na(fallback_size) && fallback_size <= max_bytes),
      size_bytes = fallback_size,
      quality = WEBP_MIN_QUALITY,
      width = dims$width,
      height = dims$height
    ))
  }

  list(
    within_limit = TRUE,
    size_bytes = best_size,
    quality = best_quality,
    width = dims$width,
    height = dims$height
  )
}

write_webp_with_size_limit <- function(image, dest_path, quality = WEBP_QUALITY, max_bytes = WEBP_MAX_BYTES) {
  temp_path <- tempfile(fileext = ".webp")
  best_path <- tempfile(fileext = ".webp")
  final_path <- tempfile(fileext = ".webp")

  on.exit(unlink(c(temp_path, best_path, final_path), force = TRUE), add = TRUE)

  primary_attempt <- fit_webp_candidate(
    image = image,
    quality = quality,
    max_bytes = max_bytes,
    temp_path = temp_path,
    best_path = best_path
  )

  final_attempt <- primary_attempt
  final_scale <- 1
  used_resize_fallback <- FALSE

  if (isTRUE(primary_attempt$within_limit) && file.exists(best_path)) {
    file.copy(best_path, final_path, overwrite = TRUE)
  }

  if (!isTRUE(primary_attempt$within_limit)) {
    coarse_scales <- c(seq(0.95, 0.20, by = -0.05), 0.15, 0.10)
    last_failed_scale <- 1
    successful_scale <- NA_real_
    successful_attempt <- NULL

    for (scale in coarse_scales) {
      resized_image <- resize_webp_fallback_image(image, scale)
      attempt <- fit_webp_candidate(
        image = resized_image,
        quality = quality,
        max_bytes = max_bytes,
        temp_path = temp_path,
        best_path = best_path
      )

      if (isTRUE(attempt$within_limit)) {
        successful_scale <- scale
        successful_attempt <- attempt
        file.copy(best_path, final_path, overwrite = TRUE)
        break
      }

      last_failed_scale <- scale
      final_attempt <- attempt
    }

    if (!is.null(successful_attempt)) {
      used_resize_fallback <- TRUE
      final_attempt <- successful_attempt
      final_scale <- successful_scale

      refine_start <- round(last_failed_scale - 0.01, 2)
      refine_end <- round(successful_scale + 0.01, 2)

      if (refine_start >= refine_end) {
        refine_scales <- seq(from = refine_start, to = refine_end, by = -0.01)

        for (scale in refine_scales) {
          resized_image <- resize_webp_fallback_image(image, scale)
          attempt <- fit_webp_candidate(
            image = resized_image,
            quality = quality,
            max_bytes = max_bytes,
            temp_path = temp_path,
            best_path = best_path
          )

          if (isTRUE(attempt$within_limit)) {
            final_attempt <- attempt
            final_scale <- scale
            file.copy(best_path, final_path, overwrite = TRUE)
            break
          }
        }
      }
    }
  }

  if (!isTRUE(final_attempt$within_limit) || !file.exists(final_path)) {
    return(list(
      success = FALSE,
      within_limit = FALSE,
      size_bytes = final_attempt$size_bytes,
      quality = final_attempt$quality,
      width = final_attempt$width,
      height = final_attempt$height,
      scale = final_scale,
      used_resize_fallback = used_resize_fallback
    ))
  }

  success <- file.copy(final_path, dest_path, overwrite = TRUE)

  list(
    success = isTRUE(success) && file.exists(dest_path),
    within_limit = TRUE,
    size_bytes = final_attempt$size_bytes,
    quality = final_attempt$quality,
    width = final_attempt$width,
    height = final_attempt$height,
    scale = final_scale,
    used_resize_fallback = used_resize_fallback
  )
}

write_export_asset <- function(source_path, dest_path, asset_format = "jpg", quality = NA_integer_) {
  dir.create(dirname(dest_path), recursive = TRUE, showWarnings = FALSE)

  if (identical(asset_format, "jpg")) {
    success <- file.copy(source_path, dest_path, overwrite = TRUE, copy.mode = TRUE)
    return(isTRUE(success) && file.exists(dest_path))
  }

  if (identical(asset_format, "webp")) {
    image <- tryCatch(
      normalize_webp_portrait(source_path),
      error = function(e) {
        abort_export(paste(
          "Failed to normalize portrait for WebP export:",
          source_path,
          "-",
          trimws(gsub("[\r\n]+", " ", conditionMessage(e)))
        ))
      }
    )

    result <- tryCatch(
      write_webp_with_size_limit(
        image = image,
        dest_path = dest_path,
        quality = quality,
        max_bytes = WEBP_MAX_BYTES
      ),
      error = function(e) {
        abort_export(paste(
          "Failed to write WebP portrait asset:",
          source_path,
          "-",
          trimws(gsub("[\r\n]+", " ", conditionMessage(e)))
        ))
      }
    )

    if (!isTRUE(result$success)) {
      return(FALSE)
    }

    if (!isTRUE(result$within_limit)) {
      abort_export(
        paste(
          "Could not compress portrait to 15 KB or less after portrait-prior cropping to the 427x602 ratio, even with fallback resize:",
          source_path,
          paste0("(quality=", result$quality, ", bytes=", as.integer(round(result$size_bytes)), ")")
        )
      )
    }

    return(TRUE)
  }

  abort_export(paste("Unsupported export asset format:", asset_format))
}

copy_export_portraits <- function(export_df, export_root, asset_format = "jpg", quality = NA_integer_) {
  copied <- 0L

  # Copy portraits into a flat, deterministic export layout so downstream tools
  # do not need to know the original district/party folder structure.
  for (row_idx in seq_len(nrow(export_df))) {
    source_path <- export_df$source_image_abs_path[[row_idx]]
    dest_path <- file.path(export_root, export_df$stickerImage[[row_idx]])

    success <- write_export_asset(
      source_path = source_path,
      dest_path = dest_path,
      asset_format = asset_format,
      quality = quality
    )

    if (!isTRUE(success) || !file.exists(dest_path)) {
      abort_export(paste("Failed to write portrait asset:", source_path))
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
  bundle_root,
  asset_extension,
  candidate_only_note = TRUE
) {
  bundle_root <- gsub("\\\\", "/", bundle_root)
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
    "# Candidate Album Export Bundle",
    "",
    "## Snapshot",
    paste0("- Candidates exported: `", export_count, "`"),
    paste0("- Parties represented: `", party_count, "`"),
    paste0("- Portrait assets written: `", image_count, "`"),
    paste0("- Bundle root: `", bundle_root, "/`"),
    "",
    "## Files",
    "- `candidates.json`: album-ready sticker dataset using the `_examples/candidates.json` contract.",
    "- `images/candidates/`: flattened portrait assets referenced by `candidates.json`.",
    "- `manifests/export_manifest.csv`: owner-facing truth table with source fields, derived fields, and optional QC columns.",
    "- `manifests/party_manifest.csv`: party metadata with `partido_id`, display name, slug, candidate count, and `url_logo_partido`.",
    "",
    "## Field Mapping",
    "- `id`: `cand_...`, keeping the source candidate id semantics while shortening long office-aware ids to satisfy the downstream 32-character limit.",
    "- `name`, `party`, `region`: title-cased display labels derived from `output/candidatos.csv`.",
    "- `type`: normalized office type carried from `output/candidatos.csv`.",
    "- `bioShort`: `<Type> - <Region>` or `<Type>` when region is missing.",
    "- `sentenciado`: `TRUE` if the candidate appears in `output/relacion_sentencias.csv`, otherwise `FALSE`.",
    "- `trabaja`: `TRUE` if the candidate appears in `output/experiencia_laboral.csv`, otherwise `FALSE`.",
    "- `ingresos`: integer soles parsed from `ingresos_total_ingresos`; blank in the manifest and `null` in JSON when missing.",
    paste0(
      "- `stickerImage` and `portraitImage`: both point to the same exported `",
      asset_extension,
      "` portrait asset."
    ),
    "",
    "## Notes",
    qc_status_line,
    party_note_line,
    "- Portrait inclusion is driven by the local `output/images/` tree, not `ruta_imagen`, because some candidate rows do not persist that field.",
    "- Sort order is deterministic: region, then party, then candidate name, then source ID.",
    sep = "\n"
  )
}

webp_write_supported <- function() {
  temp_path <- tempfile(fileext = ".webp")
  on.exit(unlink(temp_path, force = TRUE), add = TRUE)

  tryCatch({
    magick::image_write(
      magick::image_blank(width = 2, height = 2, color = "white"),
      path = temp_path,
      format = "webp",
      quality = WEBP_QUALITY
    )
    file.exists(temp_path)
  }, error = function(e) FALSE)
}

write_export_bundle <- function(
  export_df,
  qc_lookup,
  export_root,
  asset_extension = ".jpg",
  asset_format = "jpg",
  asset_quality = NA_integer_,
  bundle_label = "JPG"
) {
  bundle_paths <- rebuild_export_dirs(export_root)

  bundle_df <- export_df %>%
    dplyr::mutate(
      stickerImage = portable_rel_path("images", "candidates", paste0(.data$export_id, asset_extension)),
      portraitImage = .data$stickerImage
    )

  image_count <- copy_export_portraits(
    bundle_df,
    export_root = export_root,
    asset_format = asset_format,
    quality = asset_quality
  )

  json_df <- bundle_df %>%
    dplyr::transmute(
      id = .data$export_id,
      type = .data$type,
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
    function(id, type, name, party, region, bioShort, stickerImage, portraitImage, sentenciado, ingresos, trabaja) {
      list(
        id = id,
        type = type,
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
    path = bundle_paths$export_json_path,
    pretty = TRUE,
    auto_unbox = TRUE,
    na = "null",
    null = "null"
  )

  export_manifest <- bundle_df %>%
    dplyr::transmute(
      sort_order = .data$sort_order,
      id = .data$export_id,
      source_id_candidato = .data$id_candidato,
      source_partido_id = .data$partido_id,
      type = .data$type,
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
      source_cargo_postula_raw = .data$cargo_postula,
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

  write_csv_chr(export_manifest, bundle_paths$export_manifest_path)

  party_manifest <- bundle_df %>%
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

  write_csv_chr(party_manifest, bundle_paths$party_manifest_path)

  qc_matched_count <- export_manifest %>%
    dplyr::filter(!is.na(.data$qc_processing_status) | !is.na(.data$qc_quality_score) | !is.na(.data$qc_eligible_all)) %>%
    nrow()

  writeLines(
    build_import_guide(
      export_count = nrow(bundle_df),
      party_count = nrow(party_manifest),
      image_count = image_count,
      qc_available = qc_lookup$available,
      qc_matched_count = qc_matched_count,
      bundle_root = export_root,
      asset_extension = sub("^\\.", "", asset_extension)
    ),
    bundle_paths$import_guide_path,
    useBytes = TRUE
  )

  if (image_count != nrow(bundle_df)) {
    abort_export("The number of written portrait assets does not match the export row count.")
  }

  export_asset_exists <- file.exists(file.path(export_root, bundle_df$stickerImage))
  if (!all(export_asset_exists)) {
    abort_export("At least one exported portrait asset is missing after write.")
  }

  json_ids_unique <- length(unique(json_df$id)) == nrow(json_df)
  if (!isTRUE(json_ids_unique)) {
    abort_export("JSON IDs are not unique.")
  }

  log_message(paste(bundle_label, "bundle written to:", normalize_export_path(export_root)))

  list(
    bundle_label = bundle_label,
    export_root = export_root,
    image_count = image_count,
    party_count = nrow(party_manifest)
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

if (!"type" %in% names(candidate_df)) {
  candidate_df$type <- NA_character_
}

if (!"cargo_postula" %in% names(candidate_df)) {
  candidate_df$cargo_postula <- NA_character_
}

candidate_df <- candidate_df %>%
  dplyr::mutate(
    type = purrr::pmap_chr(
      list(.data$type, .data$cargo_postula),
      function(type_value, cargo_value) {
        coalesce_chr(type_value, derive_candidate_type(cargo_value))
      }
    ),
    export_name = purrr::map_chr(.data$nombre, display_title),
    export_party = purrr::map_chr(.data$partido_politico, display_title)
  )

if (nrow(candidate_df) == 0) {
  abort_export("No candidate rows remained after removing empty ids.")
}

incomplete_export_rows <- candidate_df %>%
  dplyr::filter(
    is.na(.data$export_name) |
      is.na(.data$export_party) |
      is.na(.data$type) |
      !nzchar(.data$type)
  )

if (nrow(incomplete_export_rows) > 0) {
  log_message(
    paste(
      "Dropping",
      nrow(incomplete_export_rows),
      "candidate row(s) with missing export-critical fields.",
      "These are usually stale legacy rows kept beside newer typed records."
    )
  )
}

candidate_df <- candidate_df %>%
  dplyr::filter(
    !is.na(.data$export_name),
    !is.na(.data$export_party),
    !is.na(.data$type),
    nzchar(.data$type)
  )

if (nrow(candidate_df) == 0) {
  abort_export("No export-ready candidate rows remained after removing incomplete legacy rows.")
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
  log_message(
    paste(
      "Ignoring",
      length(unknown_portraits),
      "portrait file(s) that do not map to an export-ready candidate row."
    )
  )
}

export_df <- candidate_df %>%
  # Build one clean owner-facing row per candidate by joining the flat tables,
  # portrait inventory, and optional QC metadata.
  dplyr::inner_join(portrait_lookup, by = c("id_candidato" = "source_id_candidato")) %>%
  dplyr::left_join(sentence_ids, by = "id_candidato") %>%
  dplyr::left_join(work_ids, by = "id_candidato") %>%
  dplyr::left_join(qc_lookup$data, by = "id_candidato") %>%
  dplyr::mutate(
    export_id = purrr::pmap_chr(
      list(.data$id_candidato, .data$type),
      build_export_id
    ),
    type = purrr::map_chr(.data$type, value_or_na),
    name = .data$export_name,
    party = .data$export_party,
    region = purrr::map_chr(.data$distrito_electoral, display_title),
    bioShort = dplyr::if_else(
      !is.na(.data$type) & nzchar(.data$type) & !is.na(.data$region) & nzchar(.data$region),
      paste(.data$type, .data$region, sep = " - "),
      dplyr::if_else(
        !is.na(.data$type) & nzchar(.data$type),
        .data$type,
        .data$region
      )
    ),
    sentenciado = dplyr::coalesce(.data$has_sentence, FALSE),
    trabaja = dplyr::coalesce(.data$has_work, FALSE),
    ingresos = purrr::map_int(.data$ingresos_total_ingresos, parse_income_integer)
  ) %>%
  dplyr::arrange(.data$region, .data$party, .data$type, .data$name, .data$id_candidato) %>%
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

if (any(is.na(export_df$type) | !nzchar(export_df$type))) {
  abort_export("At least one candidate is missing type after normalization.")
}

log_message("Validating portrait source files.")
validate_export_portraits(export_df)


# ======================================
# ==========  WRITE BUNDLE =============
# ======================================

if (!webp_write_supported()) {
  abort_export("WebP export is not supported by the current magick/ImageMagick build.")
}

bundle_specs <- list(
  list(
    export_root = EXPORT_ROOT_JPG,
    asset_extension = ".jpg",
    asset_format = "jpg",
    asset_quality = NA_integer_,
    bundle_label = "JPG"
  ),
  list(
    export_root = EXPORT_ROOT_WEBP,
    asset_extension = ".webp",
    asset_format = "webp",
    asset_quality = WEBP_QUALITY,
    bundle_label = "WebP"
  )
)

bundle_results <- purrr::map(bundle_specs, function(bundle_spec) {
  log_message(paste("Rebuilding", gsub("\\\\", "/", bundle_spec$export_root)))

  write_export_bundle(
    export_df = export_df,
    qc_lookup = qc_lookup,
    export_root = bundle_spec$export_root,
    asset_extension = bundle_spec$asset_extension,
    asset_format = bundle_spec$asset_format,
    asset_quality = bundle_spec$asset_quality,
    bundle_label = bundle_spec$bundle_label
  )
})


log_message(paste("Candidates exported:", nrow(export_df)))
for (bundle_result in bundle_results) {
  log_message(paste(bundle_result$bundle_label, "party manifest rows:", bundle_result$party_count))
  log_message(paste(bundle_result$bundle_label, "portrait assets written:", bundle_result$image_count))
}
log_message(paste("QC manifest detected:", if (isTRUE(qc_lookup$available)) "yes" else "no"))
