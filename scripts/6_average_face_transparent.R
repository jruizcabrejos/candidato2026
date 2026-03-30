######################################################################################
# Project: Scrapper and processing of Peru's 2026 election candidate data
# Script: 6_average_face_transparent.R - Remove white backgrounds from average faces
# Author: Jorge Ruiz-Cabrejos and Inca Slop
# Created: 2026-03-28
# Last Updated: 2026-03-28
######################################################################################

#~#~#~#~#~#~#~#~#~#~#~#~#~#~#
rm(list = ls())
#~#~#~#~#~#~#~#~#~#~#~#~#~#~#


# ================================
# ==========  SETTINGS ===========
# ================================

REQUIRED_PACKAGES <- c("magick")

AVERAGE_FACE_ROOT <- Sys.getenv(
  "JNE_AVERAGE_FACE_ROOT",
  unset = file.path("output", "average_faces")
)
MANIFEST_ROOT <- file.path(AVERAGE_FACE_ROOT, "manifests")
MANIFEST_PATH <- file.path(MANIFEST_ROOT, "transparent_manifest.csv")
TARGET_SUBDIRS <- c("filtered", "all_faces")

BACKGROUND_FUZZ <- suppressWarnings(
  as.numeric(Sys.getenv("JNE_TRANSPARENT_FUZZ", unset = "12"))
)

if (length(BACKGROUND_FUZZ) == 0L || is.na(BACKGROUND_FUZZ) || BACKGROUND_FUZZ < 0) {
  BACKGROUND_FUZZ <- 12
}

CROP_PADDING <- suppressWarnings(
  as.integer(Sys.getenv("JNE_TRANSPARENT_PADDING", unset = "6"))
)

if (length(CROP_PADDING) == 0L || is.na(CROP_PADDING) || CROP_PADDING < 0L) {
  CROP_PADDING <- 6L
}

ALPHA_THRESHOLD <- suppressWarnings(
  as.integer(Sys.getenv("JNE_TRANSPARENT_ALPHA_THRESHOLD", unset = "8"))
)

if (length(ALPHA_THRESHOLD) == 0L || is.na(ALPHA_THRESHOLD) || ALPHA_THRESHOLD < 0L) {
  ALPHA_THRESHOLD <- 8L
}

OVERWRITE_OUTPUT <- !identical(
  tolower(Sys.getenv("JNE_OVERWRITE_TRANSPARENT", unset = "true")),
  "false"
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

normalize_fs_path <- function(path) {
  normalizePath(path, winslash = "/", mustWork = FALSE)
}

list_average_face_images <- function(root_path) {
  target_dirs <- file.path(root_path, TARGET_SUBDIRS)
  target_dirs <- target_dirs[dir.exists(target_dirs)]

  if (length(target_dirs) == 0L) {
    return(character(0))
  }

  image_paths <- unlist(
    lapply(
      target_dirs,
      function(dir_path) {
        list.files(
          dir_path,
          pattern = "\\.(jpg|jpeg|png)$",
          recursive = TRUE,
          full.names = TRUE,
          ignore.case = TRUE
        )
      }
    ),
    use.names = FALSE
  )

  image_paths <- unique(normalize_fs_path(image_paths))
  image_paths <- image_paths[!grepl("/qc/", image_paths, fixed = TRUE)]
  image_paths <- image_paths[!grepl("_transparent\\.png$", image_paths, ignore.case = TRUE)]
  image_paths
}

build_transparent_output_path <- function(input_path) {
  paste0(tools::file_path_sans_ext(input_path), "_transparent.png")
}

format_fill_point <- function(x, y) {
  sprintf("+%d+%d", max(as.integer(x) - 1L, 0L), max(as.integer(y) - 1L, 0L))
}

edge_fill_points <- function(width, height) {
  x_mid <- max(as.integer(round(width / 2)), 1L)
  y_mid <- max(as.integer(round(height / 2)), 1L)

  coords <- unique(
    rbind(
      c(1L, 1L),
      c(width, 1L),
      c(1L, height),
      c(width, height),
      c(x_mid, 1L),
      c(x_mid, height),
      c(1L, y_mid),
      c(width, y_mid)
    )
  )

  apply(coords, 1, function(row_value) format_fill_point(row_value[[1]], row_value[[2]]))
}

alpha_bbox <- function(image, alpha_threshold = 8L, padding = 6L) {
  rgba_data <- magick::image_data(image, channels = "rgba")
  alpha_values <- as.integer(strtoi(as.vector(rgba_data[4, , ]), 16L))
  alpha_matrix <- matrix(
    alpha_values,
    nrow = dim(rgba_data)[2],
    ncol = dim(rgba_data)[3]
  )

  keep_x <- which(rowSums(alpha_matrix > alpha_threshold) > 0)
  keep_y <- which(colSums(alpha_matrix > alpha_threshold) > 0)

  if (length(keep_x) == 0L || length(keep_y) == 0L) {
    return(NULL)
  }

  width <- dim(rgba_data)[2]
  height <- dim(rgba_data)[3]

  list(
    x_min = max(min(keep_x) - padding, 1L),
    x_max = min(max(keep_x) + padding, width),
    y_min = max(min(keep_y) - padding, 1L),
    y_max = min(max(keep_y) + padding, height)
  )
}

crop_to_bbox <- function(image, bbox) {
  geometry <- sprintf(
    "%dx%d+%d+%d",
    bbox$x_max - bbox$x_min + 1L,
    bbox$y_max - bbox$y_min + 1L,
    bbox$x_min - 1L,
    bbox$y_min - 1L
  )

  magick::image_crop(image, geometry = geometry)
}

make_average_face_transparent <- function(input_path) {
  image <- magick::image_read(input_path)
  image <- magick::image_orient(image)
  image <- magick::image_convert(image, colorspace = "sRGB")
  source_info <- magick::image_info(image)[1, , drop = FALSE]

  for (fill_point in edge_fill_points(source_info$width[[1]], source_info$height[[1]])) {
    image <- magick::image_fill(
      image,
      color = "none",
      point = fill_point,
      fuzz = BACKGROUND_FUZZ
    )
  }

  bbox <- alpha_bbox(
    image = image,
    alpha_threshold = ALPHA_THRESHOLD,
    padding = CROP_PADDING
  )

  if (!is.null(bbox)) {
    image <- crop_to_bbox(image, bbox)
  }

  output_path <- build_transparent_output_path(input_path)

  if (!OVERWRITE_OUTPUT && file.exists(output_path)) {
    output_info <- magick::image_info(magick::image_read(output_path))[1, , drop = FALSE]

    return(
      data.frame(
        source_path = normalize_fs_path(input_path),
        transparent_path = normalize_fs_path(output_path),
        status = "skipped_existing",
        source_width = as.integer(source_info$width[[1]]),
        source_height = as.integer(source_info$height[[1]]),
        output_width = as.integer(output_info$width[[1]]),
        output_height = as.integer(output_info$height[[1]]),
        error = NA_character_,
        stringsAsFactors = FALSE
      )
    )
  }

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  magick::image_write(image, path = output_path, format = "png")
  output_info <- magick::image_info(image)[1, , drop = FALSE]

  data.frame(
    source_path = normalize_fs_path(input_path),
    transparent_path = normalize_fs_path(output_path),
    status = "written",
    source_width = as.integer(source_info$width[[1]]),
    source_height = as.integer(source_info$height[[1]]),
    output_width = as.integer(output_info$width[[1]]),
    output_height = as.integer(output_info$height[[1]]),
    error = NA_character_,
    stringsAsFactors = FALSE
  )
}


# =====================================
# ==========  MAIN EXECUTION ==========
# =====================================

average_face_root_norm <- normalize_fs_path(AVERAGE_FACE_ROOT)

if (!dir.exists(average_face_root_norm)) {
  stop(sprintf("Average-face root not found: %s", average_face_root_norm), call. = FALSE)
}

dir.create(MANIFEST_ROOT, recursive = TRUE, showWarnings = FALSE)

image_paths <- list_average_face_images(average_face_root_norm)

if (length(image_paths) == 0L) {
  log_message(sprintf("No average-face composites found under %s", average_face_root_norm))
  quit(save = "no", status = 0L)
}

log_message(sprintf("Average-face root: %s", average_face_root_norm))
log_message(sprintf("Composite images queued: %s", format(length(image_paths), big.mark = ",")))

results <- vector("list", length(image_paths))

for (index in seq_along(image_paths)) {
  input_path <- image_paths[[index]]

  if (index == 1L || index %% 50L == 0L || index == length(image_paths)) {
    log_message(sprintf("Processing image %s of %s", index, length(image_paths)))
  }

  results[[index]] <- tryCatch(
    make_average_face_transparent(input_path),
    error = function(err) {
      data.frame(
        source_path = normalize_fs_path(input_path),
        transparent_path = normalize_fs_path(build_transparent_output_path(input_path)),
        status = "error",
        source_width = NA_integer_,
        source_height = NA_integer_,
        output_width = NA_integer_,
        output_height = NA_integer_,
        error = conditionMessage(err),
        stringsAsFactors = FALSE
      )
    }
  )
}

manifest_df <- do.call(rbind, results)
utils::write.csv(manifest_df, MANIFEST_PATH, row.names = FALSE, na = "")

written_count <- sum(manifest_df$status == "written", na.rm = TRUE)
skipped_count <- sum(manifest_df$status == "skipped_existing", na.rm = TRUE)
error_count <- sum(manifest_df$status == "error", na.rm = TRUE)

log_message(sprintf("Transparent images written: %s", format(written_count, big.mark = ",")))
log_message(sprintf("Transparent images skipped: %s", format(skipped_count, big.mark = ",")))
log_message(sprintf("Transparent image errors: %s", format(error_count, big.mark = ",")))
log_message(sprintf("Manifest saved to %s", normalize_fs_path(MANIFEST_PATH)))

if (error_count > 0L) {
  stop(
    sprintf(
      "Some transparent average-face renders failed. Review %s",
      normalize_fs_path(MANIFEST_PATH)
    ),
    call. = FALSE
  )
}
