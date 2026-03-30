######################################################################################
# Project: Scrapper and processing of Peru's 2026 election candidate data
# Script: 2_backfill_saved_profiles.R - Rebuild processed outputs from saved raw HTML
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
  "RSelenium",
  "rvest",
  "xml2",
  "readr",
  "dplyr",
  "stringr",
  "purrr",
  "tibble"
)

HEADLESS <- FALSE
PROFILE_LOAD_TIMEOUT <- 15
IMAGE_LOAD_TIMEOUT <- 20
PROFILE_POLL_INTERVAL <- 0.5
PAUSE_AFTER_PROFILE_NAVIGATION <- 0.5

# Keep the same environment-variable interface as the live collector so both
# scripts can be driven the same way from the terminal.
ENV_HEADLESS <- Sys.getenv("JNE_HEADLESS", unset = "")
ENV_PROFILE_LOAD_TIMEOUT <- Sys.getenv("JNE_PROFILE_LOAD_TIMEOUT", unset = "")
ENV_IMAGE_LOAD_TIMEOUT <- Sys.getenv("JNE_IMAGE_LOAD_TIMEOUT", unset = "")


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


# ============================
# ==========  SETUP ==========
# ============================

create_env()
ensure_output_files()

if (nzchar(ENV_HEADLESS)) {
  HEADLESS <- resolve_logical_env(ENV_HEADLESS, default = HEADLESS)
}

ENV_PROFILE_LOAD_TIMEOUT <- resolve_integer_env(ENV_PROFILE_LOAD_TIMEOUT)
if (!is.na(ENV_PROFILE_LOAD_TIMEOUT)) {
  PROFILE_LOAD_TIMEOUT <- ENV_PROFILE_LOAD_TIMEOUT
}

ENV_IMAGE_LOAD_TIMEOUT <- resolve_integer_env(ENV_IMAGE_LOAD_TIMEOUT)
if (!is.na(ENV_IMAGE_LOAD_TIMEOUT)) {
  IMAGE_LOAD_TIMEOUT <- ENV_IMAGE_LOAD_TIMEOUT
}

log_message <- function(text) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), text))
}

merge_candidate_main_fields <- function(main_record, current_main = NULL) {
  main_record <- tibble::as_tibble(main_record)
  current_main <- tibble::as_tibble(current_main)

  if (nrow(current_main) == 0) {
    return(main_record)
  }

  main_url <- row_value_or_na(main_record, "url_imagen")
  current_url <- row_value_or_na(current_main, "url_imagen")

  main_record$ruta_imagen[[1]] <- coalesce_chr(
    row_value_or_na(main_record, "ruta_imagen"),
    row_value_or_na(current_main, "ruta_imagen")
  )
  main_record$url_imagen[[1]] <- coalesce_chr(
    if (is_non_placeholder_image_url(main_url)) main_url else NA_character_,
    if (is_non_placeholder_image_url(current_url)) current_url else NA_character_,
    main_url,
    current_url
  )
  main_record$url_logo_partido[[1]] <- coalesce_chr(
    row_value_or_na(main_record, "url_logo_partido"),
    row_value_or_na(current_main, "url_logo_partido")
  )

  main_record
}

append_image_recovery_error <- function(listing_row, id_candidato, message) {
  append_error_row(tibble::tibble(
    fecha = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    distrito_electoral = row_value_or_na(listing_row, "distrito_electoral"),
    codigo_distrito_electoral = row_value_or_na(listing_row, "codigo_distrito_electoral"),
    card_index = row_value_or_na(listing_row, "card_index"),
    url_hoja_vida = row_value_or_na(listing_row, "url_hoja_vida"),
    id_candidato = coalesce_chr(id_candidato),
    error = paste("Backfill image recovery:", message)
  ))
}

selenium_session <- NULL

ensure_live_recovery_session <- function() {
  # Create the browser lazily. Most backfill runs can finish from local files
  # alone, so we only pay the Selenium cost when portrait recovery needs it.
  if (!is.null(selenium_session)) {
    return(selenium_session)
  }

  browser_mode_label <- if (isTRUE(HEADLESS)) "headless" else "visible"
  log_message(paste("Starting", browser_mode_label, "Firefox RSelenium session for portrait recovery."))

  selenium_session <<- start_firefox_selenium(
    headless = HEADLESS,
    log_fn = log_message
  )

  selenium_session
}

on.exit(close_firefox_selenium(selenium_session), add = TRUE)

build_fallback_listing_row <- function(id_candidato) {
  parts <- strsplit(coalesce_chr(id_candidato), "_", fixed = TRUE)[[1]]

  partido_id <- if (length(parts) >= 1) value_or_na(parts[1]) else NA_character_
  dni <- if (length(parts) >= 2) value_or_na(parts[2]) else NA_character_
  url_hoja_vida <- if (!is.na(partido_id) && !is.na(dni)) {
    build_profile_url_from_ids(partido_id, dni, base = "https://votoinformadoia.jne.gob.pe")
  } else {
    NA_character_
  }

  build_listing_row(
    card_index = NA_integer_,
    district_row = tibble::tibble(
      codigo_distrito_electoral = NA_character_,
      distrito_electoral = NA_character_
    ),
    numero_postulacion = NA_character_,
    nombre = NA_character_,
    dni_listado = dni,
    partido_politico = NA_character_,
    cargo_postula = NA_character_,
    type = NA_character_,
    estado_inadmisible = NA_character_,
    url_imagen = NA_character_,
    url_logo_partido = NA_character_,
    url_hoja_vida = url_hoja_vida,
    card_html = NA_character_
  ) %>%
    dplyr::mutate(
      partido_id = partido_id,
      id_candidato = coalesce_chr(id_candidato)
    )
}


# =====================================
# ==========  LOAD RAW INDEX ===========
# =====================================

# The backfill step starts from whatever raw listing/profile files already exist
# locally and turns them back into the normalized flat-file bundle.
listing_files <- list.files(
  file.path("data", "raw", "listados"),
  pattern = "_candidatos\\.csv$",
  full.names = TRUE
)

profile_files <- list.files(
  file.path("data", "raw", "perfiles"),
  pattern = "\\.html$",
  full.names = TRUE
)

if (length(profile_files) == 0) {
  stop("No saved profile HTML files were found in data/raw/perfiles.", call. = FALSE)
}

listing_rows <- purrr::map_dfr(listing_files, function(path) {
  listing_df <- read_csv_chr(path)
  listing_name <- basename(path)

  if (is.null(listing_df) || nrow(listing_df) == 0) {
    return(tibble::tibble())
  }

  if (!"cargo_postula" %in% names(listing_df)) {
    listing_df$cargo_postula <- NA_character_
  }

  if (!"type" %in% names(listing_df)) {
    listing_df$type <- NA_character_
  }

  if (!"target_slug" %in% names(listing_df)) {
    listing_df$target_slug <- NA_character_
  }

  listing_df %>%
    dplyr::mutate(
      target_slug = dplyr::coalesce(
        .data$target_slug,
        dplyr::case_when(
          grepl("^senadores_", .env$listing_name) ~ "senadores",
          grepl("^presidente-vicepresidentes_", .env$listing_name) ~ "presidente-vicepresidentes",
          TRUE ~ "diputados"
        )
      ),
      type = dplyr::coalesce(
        .data$type,
        dplyr::case_when(
          grepl("^senadores_", .env$listing_name) ~ "Senador",
          grepl("^presidente-vicepresidentes_", .env$listing_name) ~ NA_character_,
          TRUE ~ "Diputado"
        )
      ),
      cargo_postula = dplyr::coalesce(
        .data$cargo_postula,
        dplyr::case_when(
          grepl("^senadores_", .env$listing_name) ~ "SENADOR",
          TRUE ~ "DIPUTADO"
        )
      ),
      partido_id = purrr::map_chr(.data$url_logo_partido, extract_party_id_from_logo_url),
      id_candidato = purrr::pmap_chr(
        list(.data$url_hoja_vida, .data$type),
        function(url_hoja_vida, type_value) {
          derive_candidate_ids(url_hoja_vida, type = type_value)$id_candidato
        }
      )
    )
})

district_lookup <- if (nrow(listing_rows) > 0) {
  listing_rows %>%
    dplyr::distinct(.data$target_slug, .data$codigo_distrito_electoral, .data$distrito_electoral) %>%
    dplyr::filter(!is.na(.data$codigo_distrito_electoral) | !is.na(.data$distrito_electoral))
} else {
  tibble::tibble(
    target_slug = character(),
    codigo_distrito_electoral = character(),
    distrito_electoral = character()
  )
}

log_message(paste("Saved listing files found:", length(listing_files)))
log_message(paste("Saved profile HTML files found:", length(profile_files)))


# ======================================
# ==========  REBUILD LOOP ==============
# ======================================

candidatos_path <- file.path("output", "candidatos.csv")
existing_candidates <- read_csv_chr(candidatos_path)

if (is.null(existing_candidates)) {
  existing_candidates <- empty_chr_tibble(candidate_output_columns)
}

rebuilt_count <- 0L
error_count <- 0L
touched_districts <- character()
image_metadata_repaired_count <- 0L
image_redownloaded_count <- 0L
image_live_attempt_count <- 0L
image_live_recovered_count <- 0L
image_unresolved_count <- 0L

for (profile_idx in seq_along(profile_files)) {
  profile_path <- profile_files[[profile_idx]]
  profile_id <- tools::file_path_sans_ext(basename(profile_path))

  # Rebuild each candidate independently so failures stay local and the rest of
  # the dataset can keep moving forward.
  log_message(paste0("Profile ", profile_idx, "/", length(profile_files), ": ", profile_id))

  listing_row <- listing_rows %>%
    dplyr::filter(.data$id_candidato == profile_id) %>%
    dplyr::slice_head(n = 1)

  if (nrow(listing_row) == 0) {
    listing_row <- build_fallback_listing_row(profile_id)
  }

  district_row <- tibble::tibble(
    target_slug = row_value_or_na(listing_row, "target_slug"),
    codigo_distrito_electoral = row_value_or_na(listing_row, "codigo_distrito_electoral"),
    distrito_electoral = row_value_or_na(listing_row, "distrito_electoral")
  )

  tryCatch({
    page <- xml2::read_html(profile_path)
    parsed_candidate <- parse_candidate_profile(
      page,
      listing_row,
      district_row,
      row_value_or_na(listing_row, "url_hoja_vida")
    )

    current_main <- existing_candidates %>%
      dplyr::filter(.data$id_candidato == parsed_candidate$ids$id_candidato) %>%
      dplyr::slice_head(n = 1)

    if (nrow(current_main) > 0) {
      parsed_candidate$main <- merge_candidate_main_fields(parsed_candidate$main, current_main)
    }

    previous_image_path <- row_value_or_na(current_main, "ruta_imagen")
    image_recovery <- repair_candidate_image_offline(parsed_candidate$main, current_main)
    parsed_candidate$main <- image_recovery$main

    # Prefer offline recovery first: reuse an existing JPG, redownload from a
    # stored URL, and only then reach for a live browser refresh.
    if (identical(image_recovery$status, "existing") &&
        (is.na(previous_image_path) || !file.exists(previous_image_path))) {
      image_metadata_repaired_count <- image_metadata_repaired_count + 1L
      log_message("    Portrait metadata repaired from an existing JPG on disk.")
    } else if (identical(image_recovery$status, "downloaded")) {
      image_redownloaded_count <- image_redownloaded_count + 1L
      log_message("    Portrait redownloaded from the stored image URL.")
    } else if (identical(image_recovery$status, "existing_placeholder")) {
      log_message("    Existing portrait file found, but the stored portrait URL is a placeholder. Retrying live.")
    } else if (image_recovery$needs_live_refresh) {
      log_message(paste("    Portrait needs live refresh:", image_recovery$message))
    }

    if (isTRUE(image_recovery$needs_live_refresh)) {
      image_live_attempt_count <- image_live_attempt_count + 1L

      live_result <- tryCatch({
        session <- ensure_live_recovery_session()

        fetch_live_profile_for_image(
          session$client,
          coalesce_chr(
            row_value_or_na(parsed_candidate$main, "url_hoja_vida"),
            row_value_or_na(listing_row, "url_hoja_vida")
          ),
          profile_load_timeout = PROFILE_LOAD_TIMEOUT,
          image_load_timeout = IMAGE_LOAD_TIMEOUT,
          poll_interval = PROFILE_POLL_INTERVAL,
          pause_seconds = PAUSE_AFTER_PROFILE_NAVIGATION
        )
      }, error = function(e) e)

      if (inherits(live_result, "error")) {
        image_unresolved_count <- image_unresolved_count + 1L
        append_image_recovery_error(
          listing_row,
          parsed_candidate$ids$id_candidato,
          live_result$message
        )
        log_message(paste("    Portrait recovery unresolved:", live_result$message))
      } else {
        save_profile_snapshot(parsed_candidate$ids$id_candidato, live_result$page_html)

        refreshed_page <- xml2::read_html(live_result$page_html)
        refreshed_candidate <- parse_candidate_profile(
          refreshed_page,
          listing_row,
          district_row,
          coalesce_chr(live_result$url_hoja_vida, row_value_or_na(listing_row, "url_hoja_vida"))
        )

        refreshed_candidate$main <- merge_candidate_main_fields(refreshed_candidate$main, current_main)
        refreshed_candidate$main$url_hoja_vida[[1]] <- coalesce_chr(
          live_result$url_hoja_vida,
          row_value_or_na(refreshed_candidate$main, "url_hoja_vida"),
          row_value_or_na(listing_row, "url_hoja_vida")
        )
        refreshed_candidate$main$url_imagen[[1]] <- coalesce_chr(
          live_result$url_imagen,
          row_value_or_na(refreshed_candidate$main, "url_imagen")
        )

        refreshed_image_path <- download_candidate_image(
          refreshed_candidate$main$url_imagen[[1]],
          build_image_path(refreshed_candidate$main[1, , drop = FALSE]),
          overwrite = TRUE
        )

        if (is.na(refreshed_image_path)) {
          image_unresolved_count <- image_unresolved_count + 1L
          append_image_recovery_error(
            listing_row,
            refreshed_candidate$ids$id_candidato,
            "Live profile refresh succeeded, but the portrait download still failed."
          )
          log_message("    Portrait recovery unresolved: live profile refresh succeeded, but the portrait download still failed.")
          parsed_candidate <- refreshed_candidate
        } else {
          refreshed_candidate$main$ruta_imagen[[1]] <- refreshed_image_path
          parsed_candidate <- refreshed_candidate
          image_live_recovered_count <- image_live_recovered_count + 1L
          log_message("    Portrait recovered via live profile refresh.")
        }
      }
    }

    replace_rows_by_candidate(
      parsed_candidate$main,
      candidatos_path,
      parsed_candidate$ids$id_candidato,
      default_cols = candidate_output_columns
    )

    for (section_name in names(section_file_map)) {
      section_df <- parsed_candidate$sections[[section_name]]
      section_path <- section_file_map[[section_name]]
      section_defaults <- section_default_columns[[section_name]]

      replace_rows_by_candidate(
        section_df,
        section_path,
        parsed_candidate$ids$id_candidato,
        default_cols = section_defaults
      )
    }

    mark_candidate_completed(parsed_candidate$main)

    existing_candidates <- existing_candidates %>%
      dplyr::filter(.data$id_candidato != parsed_candidate$ids$id_candidato) %>%
      dplyr::bind_rows(parsed_candidate$main)

    district_code <- row_value_or_na(listing_row, "codigo_distrito_electoral")
    if (!is.na(district_code)) {
      touched_districts <- unique(c(touched_districts, district_code))
    }

    rebuilt_count <- rebuilt_count + 1L
  }, error = function(e) {
    error_count <<- error_count + 1L

    append_error_row(tibble::tibble(
      fecha = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      distrito_electoral = row_value_or_na(listing_row, "distrito_electoral"),
      codigo_distrito_electoral = row_value_or_na(listing_row, "codigo_distrito_electoral"),
      card_index = row_value_or_na(listing_row, "card_index"),
      url_hoja_vida = row_value_or_na(listing_row, "url_hoja_vida"),
      id_candidato = profile_id,
      error = paste("Backfill:", e$message)
    ))

    log_message(paste("  Error:", e$message))
  })
}


# ======================================
# ==========  CHECKPOINTS ===============
# ======================================

if (length(touched_districts) > 0) {
  touched_rows <- district_lookup %>%
    dplyr::filter(.data$codigo_distrito_electoral %in% touched_districts)

  for (district_idx in seq_len(nrow(touched_rows))) {
    district_row <- touched_rows[district_idx, , drop = FALSE]
    save_district_checkpoint(district_row, district_idx = NA_integer_)
  }
}

log_message(paste("Profiles rebuilt from saved HTML:", rebuilt_count))
log_message(paste("Profiles with rebuild errors:", error_count))
log_message(paste("Portrait metadata repaired from existing JPGs:", image_metadata_repaired_count))
log_message(paste("Portraits redownloaded from stored URLs:", image_redownloaded_count))
log_message(paste("Portrait live refresh attempts:", image_live_attempt_count))
log_message(paste("Portraits recovered via live refresh:", image_live_recovered_count))
log_message(paste("Portrait recovery unresolved:", image_unresolved_count))
log_message("Backfill complete.")
