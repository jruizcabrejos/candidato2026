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
  as.numeric(Sys.getenv("JNE_TRANSPARENT_FUZZ", unset = "18"))
)

if (length(BACKGROUND_FUZZ) == 0L || is.na(BACKGROUND_FUZZ) || BACKGROUND_FUZZ < 0) {
  BACKGROUND_FUZZ <- 18
}

BACKGROUND_PREBLUR_SIGMA <- suppressWarnings(
  as.numeric(Sys.getenv("JNE_TRANSPARENT_PREBLUR_SIGMA", unset = "1.2"))
)

if (length(BACKGROUND_PREBLUR_SIGMA) == 0L || is.na(BACKGROUND_PREBLUR_SIGMA) || BACKGROUND_PREBLUR_SIGMA < 0) {
  BACKGROUND_PREBLUR_SIGMA <- 1.2
}

CROP_PADDING <- suppressWarnings(
  as.integer(Sys.getenv("JNE_TRANSPARENT_PADDING", unset = "6"))
)

if (length(CROP_PADDING) == 0L || is.na(CROP_PADDING) || CROP_PADDING < 0L) {
  CROP_PADDING <- 6L
}

CROP_PADDING_RATIO <- suppressWarnings(
  as.numeric(Sys.getenv("JNE_TRANSPARENT_PADDING_RATIO", unset = "0.12"))
)

if (length(CROP_PADDING_RATIO) == 0L || is.na(CROP_PADDING_RATIO) || CROP_PADDING_RATIO < 0) {
  CROP_PADDING_RATIO <- 0.12
}

ALPHA_THRESHOLD <- suppressWarnings(
  as.integer(Sys.getenv("JNE_TRANSPARENT_ALPHA_THRESHOLD", unset = "8"))
)

if (length(ALPHA_THRESHOLD) == 0L || is.na(ALPHA_THRESHOLD) || ALPHA_THRESHOLD < 0L) {
  ALPHA_THRESHOLD <- 8L
}

MASK_BLUR_SIGMA <- suppressWarnings(
  as.numeric(Sys.getenv("JNE_TRANSPARENT_MASK_BLUR_SIGMA", unset = "2.2"))
)

if (length(MASK_BLUR_SIGMA) == 0L || is.na(MASK_BLUR_SIGMA) || MASK_BLUR_SIGMA < 0) {
  MASK_BLUR_SIGMA <- 2.2
}

MASK_LEVEL_BLACK <- suppressWarnings(
  as.numeric(Sys.getenv("JNE_TRANSPARENT_MASK_LEVEL_BLACK", unset = "14"))
)

if (length(MASK_LEVEL_BLACK) == 0L || is.na(MASK_LEVEL_BLACK) || MASK_LEVEL_BLACK < 0 || MASK_LEVEL_BLACK > 100) {
  MASK_LEVEL_BLACK <- 14
}

MASK_LEVEL_WHITE <- suppressWarnings(
  as.numeric(Sys.getenv("JNE_TRANSPARENT_MASK_LEVEL_WHITE", unset = "92"))
)

if (length(MASK_LEVEL_WHITE) == 0L || is.na(MASK_LEVEL_WHITE) || MASK_LEVEL_WHITE < 0 || MASK_LEVEL_WHITE > 100) {
  MASK_LEVEL_WHITE <- 92
}

if (MASK_LEVEL_WHITE <= MASK_LEVEL_BLACK) {
  MASK_LEVEL_WHITE <- min(100, MASK_LEVEL_BLACK + 10)
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
  x_quarter <- max(as.integer(round(width / 4)), 1L)
  x_three_quarter <- max(as.integer(round((width * 3) / 4)), 1L)
  y_quarter <- max(as.integer(round(height / 4)), 1L)
  y_three_quarter <- max(as.integer(round((height * 3) / 4)), 1L)

  coords <- unique(
    rbind(
      c(1L, 1L),
      c(width, 1L),
      c(1L, height),
      c(width, height),
      c(x_mid, 1L),
      c(x_mid, height),
      c(1L, y_mid),
      c(width, y_mid),
      c(x_quarter, 1L),
      c(x_three_quarter, 1L),
      c(x_quarter, height),
      c(x_three_quarter, height),
      c(1L, y_quarter),
      c(1L, y_three_quarter),
      c(width, y_quarter),
      c(width, y_three_quarter)
    )
  )

  apply(coords, 1, function(row_value) format_fill_point(row_value[[1]], row_value[[2]]))
}

alpha_matrix_from_image <- function(image) {
  rgba_data <- magick::image_data(image, channels = "rgba")
  alpha_values <- as.integer(strtoi(as.vector(rgba_data[4, , ]), 16L))
  matrix(
    alpha_values,
    nrow = dim(rgba_data)[2],
    ncol = dim(rgba_data)[3]
  )
}

alpha_matrix_to_mask_image <- function(alpha_matrix) {
  alpha_matrix <- round(alpha_matrix)
  alpha_matrix[alpha_matrix < 0] <- 0
  alpha_matrix[alpha_matrix > 255] <- 255
  alpha_values <- t(alpha_matrix) / 255
  mask_colors <- grDevices::rgb(alpha_values, alpha_values, alpha_values)
  dim(mask_colors) <- dim(alpha_values)

  magick::image_read(as.raster(mask_colors))
}

gray_matrix_from_image <- function(image) {
  gray_data <- magick::image_data(
    magick::image_convert(image, colorspace = "gray"),
    channels = "gray"
  )
  gray_values <- as.integer(strtoi(as.vector(gray_data[1, , ]), 16L))

  matrix(
    gray_values,
    nrow = dim(gray_data)[2],
    ncol = dim(gray_data)[3]
  )
}

rgb_matrices_from_image <- function(image) {
  rgb_data <- magick::image_data(image, channels = "rgb")

  list(
    red = matrix(
      as.integer(strtoi(as.vector(rgb_data[1, , ]), 16L)),
      nrow = dim(rgb_data)[2],
      ncol = dim(rgb_data)[3]
    ),
    green = matrix(
      as.integer(strtoi(as.vector(rgb_data[2, , ]), 16L)),
      nrow = dim(rgb_data)[2],
      ncol = dim(rgb_data)[3]
    ),
    blue = matrix(
      as.integer(strtoi(as.vector(rgb_data[3, , ]), 16L)),
      nrow = dim(rgb_data)[2],
      ncol = dim(rgb_data)[3]
    )
  )
}

rgba_image_from_source_and_alpha <- function(source_image, alpha_matrix) {
  rgb_matrices <- rgb_matrices_from_image(source_image)
  rgba_colors <- grDevices::rgb(
    t(rgb_matrices$red) / 255,
    t(rgb_matrices$green) / 255,
    t(rgb_matrices$blue) / 255,
    alpha = t(alpha_matrix) / 255
  )

  dim(rgba_colors) <- c(ncol(alpha_matrix), nrow(alpha_matrix))

  magick::image_read(as.raster(rgba_colors))
}

build_smoothed_alpha_matrix <- function(image, alpha_threshold = 8L) {
  alpha_matrix <- alpha_matrix_from_image(image)
  alpha_matrix[alpha_matrix <= alpha_threshold] <- 0L

  if (!any(alpha_matrix > 0L)) {
    return(NULL)
  }

  alpha_mask <- alpha_matrix_to_mask_image(alpha_matrix)
  alpha_mask <- magick::image_convert(alpha_mask, colorspace = "gray")

  if (MASK_BLUR_SIGMA > 0) {
    alpha_mask <- magick::image_blur(alpha_mask, radius = 0, sigma = MASK_BLUR_SIGMA)
  }

  alpha_mask <- magick::image_level(
    alpha_mask,
    black_point = MASK_LEVEL_BLACK,
    white_point = MASK_LEVEL_WHITE
  )

  gray_matrix_from_image(alpha_mask)
}

expand_bbox_to_aspect <- function(bbox, canvas_width, canvas_height, target_aspect) {
  bbox_width <- bbox$x_max - bbox$x_min + 1L
  bbox_height <- bbox$y_max - bbox$y_min + 1L

  if (is.na(target_aspect) || target_aspect <= 0) {
    return(bbox)
  }

  current_aspect <- bbox_height / bbox_width

  if (current_aspect < target_aspect) {
    target_height <- min(canvas_height, as.integer(ceiling(bbox_width * target_aspect)))
    extra <- max(target_height - bbox_height, 0L)
    add_top <- extra %/% 2L
    add_bottom <- extra - add_top

    bbox$y_min <- max(bbox$y_min - add_top, 1L)
    bbox$y_max <- min(bbox$y_max + add_bottom, canvas_height)

    remaining <- target_height - (bbox$y_max - bbox$y_min + 1L)
    if (remaining > 0L) {
      bbox$y_min <- max(bbox$y_min - remaining, 1L)
      bbox$y_max <- min(bbox$y_min + target_height - 1L, canvas_height)
    }
  } else if (current_aspect > target_aspect) {
    target_width <- min(canvas_width, as.integer(ceiling(bbox_height / target_aspect)))
    extra <- max(target_width - bbox_width, 0L)
    add_left <- extra %/% 2L
    add_right <- extra - add_left

    bbox$x_min <- max(bbox$x_min - add_left, 1L)
    bbox$x_max <- min(bbox$x_max + add_right, canvas_width)

    remaining <- target_width - (bbox$x_max - bbox$x_min + 1L)
    if (remaining > 0L) {
      bbox$x_min <- max(bbox$x_min - remaining, 1L)
      bbox$x_max <- min(bbox$x_min + target_width - 1L, canvas_width)
    }
  }

  bbox
}

alpha_bbox <- function(alpha_matrix, alpha_threshold = 8L, padding = 6L, target_aspect = NA_real_) {
  keep_x <- which(rowSums(alpha_matrix > alpha_threshold) > 0)
  keep_y <- which(colSums(alpha_matrix > alpha_threshold) > 0)

  if (length(keep_x) == 0L || length(keep_y) == 0L) {
    return(NULL)
  }

  width <- nrow(alpha_matrix)
  height <- ncol(alpha_matrix)
  subject_width <- max(keep_x) - min(keep_x) + 1L
  subject_height <- max(keep_y) - min(keep_y) + 1L
  effective_padding <- max(
    as.integer(padding),
    as.integer(round(max(subject_width, subject_height) * CROP_PADDING_RATIO))
  )

  expand_bbox_to_aspect(
    bbox = list(
      x_min = max(min(keep_x) - effective_padding, 1L),
      x_max = min(max(keep_x) + effective_padding, width),
      y_min = max(min(keep_y) - effective_padding, 1L),
      y_max = min(max(keep_y) + effective_padding, height)
    ),
    canvas_width = width,
    canvas_height = height,
    target_aspect = target_aspect
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
  source_image <- magick::image_read(input_path)
  source_image <- magick::image_orient(source_image)
  source_image <- magick::image_convert(source_image, colorspace = "sRGB")
  source_info <- magick::image_info(source_image)[1, , drop = FALSE]

  working_image <- source_image

  if (BACKGROUND_PREBLUR_SIGMA > 0) {
    working_image <- magick::image_blur(
      working_image,
      radius = 0,
      sigma = BACKGROUND_PREBLUR_SIGMA
    )
  }

  for (fill_point in edge_fill_points(source_info$width[[1]], source_info$height[[1]])) {
    working_image <- magick::image_fill(
      working_image,
      color = "none",
      point = fill_point,
      fuzz = BACKGROUND_FUZZ
    )
  }

  alpha_matrix <- build_smoothed_alpha_matrix(
    working_image,
    alpha_threshold = ALPHA_THRESHOLD
  )

  if (is.null(alpha_matrix)) {
    image <- magick::image_background(source_image, color = "white", flatten = TRUE)
  } else {
    image <- rgba_image_from_source_and_alpha(source_image, alpha_matrix)
  }

  bbox <- alpha_bbox(
    alpha_matrix = if (is.null(alpha_matrix)) alpha_matrix_from_image(image) else alpha_matrix,
    alpha_threshold = ALPHA_THRESHOLD,
    padding = CROP_PADDING,
    target_aspect = as.numeric(source_info$height[[1]]) / as.numeric(source_info$width[[1]])
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
