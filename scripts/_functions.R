######################################################################################
# Project: Scrapper and processing of Peru's 2026 election candidate data
# Script: _functions.R - Shared helper functions for collection and processing
# Author: Jorge Ruiz-Cabrejos and Inca Slop
# Created: 2026-03-25
# Last Updated: 2026-03-25
######################################################################################

# ================================
# ==========  PACKAGES ===========
# ================================

install_if_missing <- function(pkgs) {
  # Use a project-local library so collaborators can run the pipeline without
  # modifying their global R installation.
  target_lib <- resolve_r_library()
  installed <- rownames(installed.packages())
  missing_pkgs <- pkgs[!pkgs %in% installed]

  if (length(missing_pkgs) > 0) {
    install.packages(missing_pkgs, repos = "https://cran.rstudio.com", lib = target_lib)
  }
}

resolve_r_library <- function() {
  target_lib <- Sys.getenv("R_LIBS_USER", unset = "")

  if (!nzchar(target_lib)) {
    target_lib <- file.path(getwd(), "r_libs")
    Sys.setenv(R_LIBS_USER = target_lib)
  }

  if (!dir.exists(target_lib)) {
    dir.create(target_lib, recursive = TRUE, showWarnings = FALSE)
  }

  .libPaths(unique(c(target_lib, .libPaths())))
  target_lib
}


# =======================================
# ==========  PATHS AND FILES ===========
# =======================================

create_env <- function(base = ".") {
  # Create only the directories that the pipeline writes to directly. Git may
  # ignore many of them, but the scripts still need them during local runs.
  dirs <- c(
    file.path(base, "data"),
    file.path(base, "data", "raw"),
    file.path(base, "data", "raw", "listados"),
    file.path(base, "data", "raw", "perfiles"),
    file.path(base, "output"),
    file.path(base, "output", "images"),
    file.path(base, "output", "checkpoints")
  )

  for (dir_path in dirs) {
    if (!dir.exists(dir_path)) {
      dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
    }
  }

  invisible(dirs)
}

candidate_output_columns <- c(
  "id_candidato",
  "partido_id",
  "codigo_distrito_electoral",
  "distrito_electoral",
  "cargo_postula",
  "type",
  "dni",
  "nombre",
  "partido_politico",
  "sexo",
  "lugar_nacimiento",
  "lugar_domicilio",
  "numero_postulacion",
  "estado_inadmisible",
  "url_hoja_vida",
  "url_imagen",
  "ruta_imagen",
  "url_logo_partido",
  "educacion_basica_cuenta",
  "educacion_basica_primaria",
  "educacion_basica_secundaria",
  "ingresos_remuneracion_bruta_publico",
  "ingresos_remuneracion_bruta_privado",
  "ingresos_total_ingresos",
  "fecha_captura"
)

listing_output_columns <- c(
  "card_index",
  "codigo_distrito_electoral",
  "distrito_electoral",
  "numero_postulacion",
  "nombre",
  "dni_listado",
  "partido_politico",
  "cargo_postula",
  "type",
  "estado_inadmisible",
  "url_imagen",
  "url_logo_partido",
  "url_hoja_vida",
  "card_html"
)

section_file_map <- c(
  estudios_tecnicos = "output/estudios_tecnicos.csv",
  estudios_no_universitarios = "output/estudios_no_universitarios.csv",
  estudios_universitarios = "output/estudios_universitarios.csv",
  estudios_posgrado = "output/estudios_posgrado.csv",
  experiencia_laboral = "output/experiencia_laboral.csv",
  cargos_partidarios = "output/cargos_partidarios.csv",
  eleccion_popular = "output/eleccion_popular.csv",
  relacion_sentencias = "output/relacion_sentencias.csv",
  bienes_muebles_inmuebles = "output/bienes_muebles_inmuebles.csv",
  informacion_adicional = "output/informacion_adicional.csv",
  anotacion_marginal = "output/anotacion_marginal.csv"
)

section_default_columns <- list(
  estudios_tecnicos = c("id_candidato", "estudio", "institucion", "grado", "condicion", "anio", "detalle_original"),
  estudios_no_universitarios = c("id_candidato", "estudio", "institucion", "grado", "condicion", "anio", "detalle_original"),
  estudios_universitarios = c("id_candidato", "estudio", "institucion", "grado", "condicion", "anio", "detalle_original"),
  estudios_posgrado = c("id_candidato", "estudio", "institucion", "grado", "condicion", "anio", "detalle_original"),
  experiencia_laboral = c("id_candidato", "entidad", "cargo", "periodo", "detalle_original"),
  cargos_partidarios = c("id_candidato", "cargo", "organizacion", "periodo", "detalle_original"),
  eleccion_popular = c("id_candidato", "cargo", "organizacion", "periodo", "detalle_original"),
  relacion_sentencias = c("id_candidato", "tipo_sentencia", "materia", "resultado", "detalle_original"),
  bienes_muebles_inmuebles = c("id_candidato", "tipo_bien", "descripcion", "detalle", "valor", "detalle_original"),
  informacion_adicional = c("id_candidato", "titulo", "detalle", "detalle_original"),
  anotacion_marginal = c("id_candidato", "detalle", "detalle_original")
)

error_output_columns <- c(
  "fecha",
  "distrito_electoral",
  "codigo_distrito_electoral",
  "card_index",
  "url_hoja_vida",
  "id_candidato",
  "error"
)

candidate_status_path <- file.path("output", "candidatos_estado.csv")

candidate_status_columns <- c(
  "id_candidato",
  "estado",
  "type",
  "distrito_electoral",
  "codigo_distrito_electoral",
  "url_hoja_vida",
  "fecha_captura"
)

checkpoint_bundle_paths <- c(
  candidatos = file.path("output", "candidatos.csv"),
  section_file_map,
  errores_scrape = file.path("output", "errores_scrape.csv"),
  candidatos_estado = candidate_status_path
)

empty_chr_tibble <- function(columns) {
  tibble::as_tibble(
    stats::setNames(
      replicate(length(columns), character(0), simplify = FALSE),
      columns
    )
  )
}

ensure_table_columns <- function(path, required_cols) {
  existing <- read_csv_chr(path)

  if (is.null(existing)) {
    readr::write_csv(empty_chr_tibble(required_cols), path, na = "")
    return(invisible(empty_chr_tibble(required_cols)))
  }

  missing_cols <- setdiff(required_cols, names(existing))

  for (col_name in missing_cols) {
    existing[[col_name]] <- NA_character_
  }

  ordered_cols <- c(required_cols, setdiff(names(existing), required_cols))
  existing <- existing[, ordered_cols, drop = FALSE]
  write_csv_chr(existing, path)

  invisible(existing)
}

ensure_output_files <- function() {
  candidatos_path <- file.path(".", "output", "candidatos.csv")
  errores_path <- file.path(".", "output", "errores_scrape.csv")

  ensure_table_columns(candidatos_path, candidate_output_columns)
  ensure_table_columns(errores_path, error_output_columns)
  ensure_table_columns(candidate_status_path, candidate_status_columns)

  for (section_name in names(section_file_map)) {
    section_path <- section_file_map[[section_name]]
    default_cols <- section_default_columns[[section_name]]
    ensure_table_columns(section_path, default_cols)
  }

  invisible(candidatos_path)
}


# ======================================
# ==========  TEXT UTILITIES ===========
# ======================================

timestamp_msg <- function(text) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), text))
}

value_or_na <- function(x) {
  if (length(x) == 0 || is.null(x)) {
    return(NA_character_)
  }

  x <- as.character(x[[1]])
  x <- stringr::str_squish(x)

  if (!nzchar(x)) {
    return(NA_character_)
  }

  x
}

coalesce_chr <- function(...) {
  values <- list(...)

  for (value in values) {
    if (!is.null(value) && length(value) > 0) {
      current <- as.character(value[[1]])
      current <- stringr::str_squish(current)

      if (nzchar(current) && !is.na(current)) {
        return(current)
      }
    }
  }

  NA_character_
}

row_value_or_na <- function(df, column) {
  if (is.null(df) || nrow(df) == 0 || !column %in% names(df)) {
    return(NA_character_)
  }

  value_or_na(df[[column]][[1]])
}

clean_name_es <- function(x) {
  x %>%
    iconv(to = "ASCII//TRANSLIT") %>%
    tolower() %>%
    gsub("[^a-z0-9]+", "_", ., perl = TRUE) %>%
    gsub("(^_+|_+$)", "", ., perl = TRUE) %>%
    gsub("_+", "_", ., perl = TRUE)
}

slugify_path <- function(x) {
  x %>%
    iconv(to = "ASCII//TRANSLIT") %>%
    tolower() %>%
    gsub("[^a-z0-9]+", "-", ., perl = TRUE) %>%
    gsub("(^-+|-+$)", "", ., perl = TRUE) %>%
    gsub("-+", "-", ., perl = TRUE)
}

make_absolute_url <- function(x, base = "https://votoinformado.jne.gob.pe") {
  if (is.null(x) || length(x) == 0) {
    return(NA_character_)
  }

  x <- value_or_na(x)

  if (is.na(x)) {
    return(NA_character_)
  }

  if (grepl("^https?://", x, ignore.case = TRUE)) {
    return(x)
  }

  if (grepl("^//", x)) {
    return(paste0("https:", x))
  }

  if (grepl("^/", x)) {
    return(paste0(base, x))
  }

  paste0(base, "/", x)
}

is_placeholder_image <- function(url) {
  url <- value_or_na(url)

  if (is.na(url) || !nzchar(url)) {
    return(TRUE)
  }

  any(vapply(
    c(
      "assets/images/logojne\\.png(?:\\?.*)?$",
      "(^|/)foto-perfil\\.png(?:\\?.*)?$"
    ),
    function(pattern) grepl(pattern, url, ignore.case = TRUE, perl = TRUE),
    logical(1)
  ))
}

normalize_existing_path <- function(path) {
  path <- value_or_na(path)

  if (is.na(path) || !file.exists(path)) {
    return(NA_character_)
  }

  file_info <- suppressWarnings(file.info(path))
  file_size <- as.numeric(file_info$size[[1]])

  if (is.na(file_size) || file_size <= 0) {
    return(NA_character_)
  }

  normalizePath(path, winslash = "/", mustWork = FALSE)
}

is_non_placeholder_image_url <- function(url) {
  url <- value_or_na(url)
  !is.na(url) && !is_placeholder_image(url)
}

has_placeholder_image_url <- function(...) {
  urls <- unlist(list(...), use.names = FALSE)

  any(vapply(urls, function(url) {
    url <- value_or_na(url)
    !is.na(url) && is_placeholder_image(url)
  }, logical(1)))
}

candidate_image_download_url <- function(main_record, current_main = NULL) {
  current_url <- row_value_or_na(current_main, "url_imagen")
  main_url <- row_value_or_na(main_record, "url_imagen")

  coalesce_chr(
    if (is_non_placeholder_image_url(main_url)) main_url else NA_character_,
    if (is_non_placeholder_image_url(current_url)) current_url else NA_character_
  )
}

repair_candidate_image_offline <- function(main_record, current_main = NULL) {
  main_record <- tibble::as_tibble(main_record)
  current_main <- tibble::as_tibble(current_main)

  expected_path <- build_image_path(main_record[1, , drop = FALSE])
  expected_path_normalized <- normalize_existing_path(expected_path)
  current_path_normalized <- normalize_existing_path(row_value_or_na(current_main, "ruta_imagen"))
  main_path_normalized <- normalize_existing_path(row_value_or_na(main_record, "ruta_imagen"))

  existing_path <- coalesce_chr(
    expected_path_normalized,
    main_path_normalized,
    current_path_normalized
  )

  placeholder_detected <- has_placeholder_image_url(
    row_value_or_na(main_record, "url_imagen"),
    row_value_or_na(current_main, "url_imagen")
  )

  if (!is.na(existing_path)) {
    main_record$ruta_imagen[[1]] <- existing_path

    if (!is_non_placeholder_image_url(main_record$url_imagen[[1]])) {
      main_record$url_imagen[[1]] <- coalesce_chr(
        candidate_image_download_url(main_record, current_main),
        row_value_or_na(main_record, "url_imagen"),
        row_value_or_na(current_main, "url_imagen")
      )
    }

    return(list(
      main = main_record,
      status = if (placeholder_detected) "existing_placeholder" else "existing",
      needs_live_refresh = placeholder_detected,
      message = if (placeholder_detected) {
        "Existing JPG found, but the stored portrait URL is a placeholder."
      } else {
        "Existing JPG found on disk."
      },
      expected_path = normalizePath(expected_path, winslash = "/", mustWork = FALSE)
    ))
  }

  download_url <- candidate_image_download_url(main_record, current_main)

  if (!is.na(download_url)) {
    downloaded_path <- download_candidate_image(download_url, expected_path, overwrite = FALSE)

    if (!is.na(downloaded_path)) {
      main_record$url_imagen[[1]] <- download_url
      main_record$ruta_imagen[[1]] <- downloaded_path

      return(list(
        main = main_record,
        status = "downloaded",
        needs_live_refresh = FALSE,
        message = "Portrait redownloaded from the stored image URL.",
        expected_path = normalizePath(expected_path, winslash = "/", mustWork = FALSE)
      ))
    }

    main_record$url_imagen[[1]] <- download_url

    return(list(
      main = main_record,
      status = "download_failed",
      needs_live_refresh = TRUE,
      message = "Stored portrait URL was available, but the redownload failed.",
      expected_path = normalizePath(expected_path, winslash = "/", mustWork = FALSE)
    ))
  }

  main_record$url_imagen[[1]] <- coalesce_chr(
    row_value_or_na(main_record, "url_imagen"),
    row_value_or_na(current_main, "url_imagen")
  )

  list(
    main = main_record,
    status = if (placeholder_detected) "placeholder_missing" else "missing",
    needs_live_refresh = TRUE,
    message = if (placeholder_detected) {
      "Only placeholder portrait URLs were available."
    } else {
      "No portrait file or direct image URL was available offline."
    },
    expected_path = normalizePath(expected_path, winslash = "/", mustWork = FALSE)
  )
}

split_multiline_text <- function(text) {
  if (is.na(text) || !nzchar(text)) {
    return(character())
  }

  text %>%
    strsplit("\n", fixed = TRUE) %>%
    unlist(use.names = FALSE) %>%
    stringr::str_squish() %>%
    .[nzchar(.)]
}

compact_lines <- function(lines) {
  lines %>%
    stringr::str_squish() %>%
    .[nzchar(.)] %>%
    unique()
}


# ==========================================
# ==========  CSV PERSISTENCE ==============
# ==========================================

read_csv_chr <- function(path) {
  if (!file.exists(path)) {
    return(NULL)
  }

  readr::read_csv(path, col_types = readr::cols(.default = readr::col_character()), show_col_types = FALSE)
}

write_csv_chr <- function(df, path) {
  df <- tibble::as_tibble(df)
  df[] <- lapply(df, as.character)
  readr::write_csv(df, path, na = "")
}

replace_rows_by_candidate <- function(df_new, path, id_candidato, default_cols = NULL) {
  # Replace rows only for the current candidate so reruns stay resumable and do
  # not duplicate data when a district or profile is processed again.
  existing <- read_csv_chr(path)
  target_id <- value_or_na(id_candidato)

  if (is.null(existing)) {
    if (is.null(df_new) || nrow(df_new) == 0) {
      if (!is.null(default_cols)) {
        empty_df <- tibble::as_tibble(stats::setNames(replicate(length(default_cols), character(0), simplify = FALSE), default_cols))
        write_csv_chr(empty_df, path)
      }
      return(invisible(NULL))
    }

    write_csv_chr(df_new, path)
    return(invisible(NULL))
  }

  if ("id_candidato" %in% names(existing)) {
    existing <- existing %>% dplyr::filter(.data$id_candidato != .env$target_id)
  }

  if (!is.null(df_new) && nrow(df_new) > 0) {
    merged <- dplyr::bind_rows(existing, df_new)
  } else {
    merged <- existing
  }

  if (nrow(merged) == 0 && !is.null(default_cols)) {
    merged <- tibble::as_tibble(stats::setNames(replicate(length(default_cols), character(0), simplify = FALSE), default_cols))
  }

  write_csv_chr(merged, path)
  invisible(NULL)
}

append_error_row <- function(error_row, path = file.path("output", "errores_scrape.csv")) {
  existing <- read_csv_chr(path)
  merged <- dplyr::bind_rows(existing, tibble::as_tibble(error_row))
  write_csv_chr(merged, path)
  invisible(NULL)
}

completed_candidate_ids <- function(path = candidate_status_path) {
  status_rows <- read_csv_chr(path)

  if (is.null(status_rows) || nrow(status_rows) == 0 || !"id_candidato" %in% names(status_rows)) {
    return(character())
  }

  if ("estado" %in% names(status_rows)) {
    status_rows <- status_rows %>%
      dplyr::filter(.data$estado == "completed")
  }

  ids <- status_rows$id_candidato
  ids <- ids[!is.na(ids) & nzchar(ids)]
  unique(ids)
}

mark_candidate_completed <- function(main_row, path = candidate_status_path) {
  main_row <- tibble::as_tibble(main_row)

  if (nrow(main_row) == 0) {
    return(invisible(NULL))
  }

  status_row <- tibble::tibble(
    id_candidato = coalesce_chr(main_row$id_candidato[[1]]),
    estado = "completed",
    type = coalesce_chr(main_row$type[[1]]),
    distrito_electoral = coalesce_chr(main_row$distrito_electoral[[1]]),
    codigo_distrito_electoral = coalesce_chr(main_row$codigo_distrito_electoral[[1]]),
    url_hoja_vida = coalesce_chr(main_row$url_hoja_vida[[1]]),
    fecha_captura = coalesce_chr(main_row$fecha_captura[[1]], format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
  )

  replace_rows_by_candidate(
    status_row,
    path,
    status_row$id_candidato[[1]],
    default_cols = candidate_status_columns
  )

  invisible(status_row)
}

save_district_checkpoint <- function(district_row, district_idx = NA_integer_, root_dir = file.path("output", "checkpoints")) {
  # Checkpoints are convenience snapshots of the flat files after each district,
  # useful when a long live run is interrupted partway through.
  district_row <- tibble::as_tibble(district_row)

  target_slug <- coalesce_chr(district_row$target_slug[[1]])
  district_code <- coalesce_chr(district_row$codigo_distrito_electoral[[1]])
  district_name <- coalesce_chr(district_row$distrito_electoral[[1]], "sin-distrito")

  prefix <- ""
  if (!is.na(district_idx)) {
    prefix <- sprintf("%03d_", as.integer(district_idx))
  }

  checkpoint_dir <- file.path(
    root_dir,
    paste0(
      prefix,
      paste(
        {
          checkpoint_parts <- c(target_slug, district_code, slugify_path(district_name))
          checkpoint_parts[!is.na(checkpoint_parts) & nzchar(checkpoint_parts)]
        },
        collapse = "_"
      )
    )
  )

  if (!dir.exists(checkpoint_dir)) {
    dir.create(checkpoint_dir, recursive = TRUE, showWarnings = FALSE)
  }

  copied_files <- character()

  for (source_path in unique(unname(checkpoint_bundle_paths))) {
    if (!file.exists(source_path)) {
      next
    }

    dest_path <- file.path(checkpoint_dir, basename(source_path))

    if (isTRUE(file.copy(source_path, dest_path, overwrite = TRUE, copy.mode = TRUE))) {
      copied_files <- c(copied_files, basename(source_path))
    }
  }

  candidatos <- read_csv_chr(file.path("output", "candidatos.csv"))
  district_candidate_count <- 0L

  if (!is.null(candidatos) && nrow(candidatos) > 0) {
    same_district <- rep(FALSE, nrow(candidatos))

    if ("codigo_distrito_electoral" %in% names(candidatos) && !is.na(district_code)) {
      same_district <- same_district | candidatos$codigo_distrito_electoral == district_code
    }

    if ("distrito_electoral" %in% names(candidatos) && !is.na(district_name)) {
      same_district <- same_district | candidatos$distrito_electoral == district_name
    }

    district_candidate_count <- sum(same_district, na.rm = TRUE)
  }

  checkpoint_info <- tibble::tibble(
    target_slug = target_slug,
    distrito_electoral = district_name,
    codigo_distrito_electoral = district_code,
    checkpoint_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    candidatos_en_csv = as.character(district_candidate_count),
    archivos_copiados = paste(copied_files, collapse = "; ")
  )

  readr::write_csv(checkpoint_info, file.path(checkpoint_dir, "checkpoint_info.csv"), na = "")

  invisible(list(
    dir = checkpoint_dir,
    files = copied_files,
    district_candidate_count = district_candidate_count
  ))
}


# ==========================================
# ==========  HTML PARSING =================
# ==========================================

safe_html_text <- function(node) {
  if (length(node) == 0 || is.null(node) || inherits(node, "xml_missing")) {
    return(NA_character_)
  }

  text <- tryCatch(rvest::html_text2(node), error = function(e) NA_character_)
  value_or_na(text)
}

safe_html_attr <- function(node, attr) {
  if (length(node) == 0 || is.null(node) || inherits(node, "xml_missing")) {
    return(NA_character_)
  }

  attr_value <- tryCatch(rvest::html_attr(node, attr), error = function(e) NA_character_)
  value_or_na(attr_value)
}

section_title_key <- function(section_title) {
  key <- clean_name_es(section_title)

  if (is.na(key)) {
    return(NA_character_)
  }

  if (grepl("^informacion_general$", key)) {
    return("informacion_general")
  }

  if (grepl("^formacion_academica$", key)) {
    return("formacion_academica")
  }

  if (grepl("^educacion_basica$", key)) {
    return("educacion_basica")
  }

  if (grepl("^estudios_tecnicos$", key)) {
    return("estudios_tecnicos")
  }

  if (grepl("^estudios_no_universitarios$", key)) {
    return("estudios_no_universitarios")
  }

  if (grepl("^estudios_universitarios$", key)) {
    return("estudios_universitarios")
  }

  if (grepl("^estudios_de_posgrado$|^estudios_posgrado$", key)) {
    return("estudios_posgrado")
  }

  if (grepl("^experiencia_laboral$", key)) {
    return("experiencia_laboral")
  }

  if (grepl("^cargos_partidarios_o_de_eleccion_popular$|^trayectoria_politica$", key)) {
    return("trayectoria_politica")
  }

  if (grepl("^cargos_partidarios$", key)) {
    return("cargos_partidarios")
  }

  if (grepl("^eleccion_popular$|^cargos_de_eleccion_popular$", key)) {
    return("eleccion_popular")
  }

  if (grepl("^relacion_de_sentencias$|^declaracion_de_sentencias_firmes$", key)) {
    return("declaracion_de_sentencias_firmes")
  }

  if (grepl("^ingresos_de_bienes_y_rentas$|^ingreso_de_bienes_y_rentas$", key)) {
    return("ingreso_de_bienes_y_rentas")
  }

  if (grepl("^bienes_muebles_inmuebles$", key)) {
    return("bienes_muebles_inmuebles")
  }

  if (grepl("^informacion_adicional$", key)) {
    return("informacion_adicional")
  }

  if (grepl("^anotacion_marginal$", key)) {
    return("anotacion_marginal")
  }

  key
}

extract_profile_summary_node <- function(page) {
  coalesce_chr_node <- function(...) {
    nodes <- list(...)

    for (node in nodes) {
      if (length(node) > 0 && !inherits(node, "xml_missing")) {
        return(node)
      }
    }

    xml2::xml_missing()
  }

  coalesce_chr_node(
    rvest::html_element(page, "#content-postulacion-comparacion"),
    rvest::html_element(page, "main #content-postulacion-comparacion"),
    rvest::html_element(page, "div[id='content-postulacion-comparacion']")
  )
}

extract_profile_name <- function(page) {
  summary_node <- extract_profile_summary_node(page)
  name_candidates <- compact_lines(c(
    safe_html_text(rvest::html_element(summary_node, "h2")),
    safe_html_text(rvest::html_element(page, "main h2[translate='no']")),
    safe_html_text(rvest::html_element(page, "main h1"))
  ))

  name_candidates <- name_candidates[
    !grepl("^candidatos_eg_2026$", clean_name_es(name_candidates))
  ]

  value_or_na(name_candidates[1])
}

extract_district_options <- function(page) {
  options <- rvest::html_elements(page, "#departamento option")

  if (length(options) == 0) {
    return(tibble::tibble(
      codigo_distrito_electoral = character(),
      distrito_electoral = character()
    ))
  }

  tibble::tibble(
    codigo_distrito_electoral = purrr::map_chr(options, ~ value_or_na(rvest::html_attr(.x, "value"))),
    distrito_electoral = purrr::map_chr(options, ~ value_or_na(rvest::html_text2(.x)))
  ) %>%
    dplyr::filter(!is.na(.data$codigo_distrito_electoral), !is.na(.data$distrito_electoral))
}

extract_profile_url_from_html <- function(html_fragment) {
  if (is.na(html_fragment) || !nzchar(html_fragment)) {
    return(NA_character_)
  }

  url_match <- stringr::str_match(html_fragment, "(/hoja-vida/[0-9]+/[0-9]+)")
  make_absolute_url(url_match[, 2])
}

extract_party_id_from_logo_url <- function(url_logo_partido) {
  url_logo_partido <- value_or_na(url_logo_partido)

  if (is.na(url_logo_partido)) {
    return(NA_character_)
  }

  matched <- stringr::str_match(url_logo_partido, "(?:GetSimbolo/|LogoOp/|/)([0-9]+)(?:\\.[A-Za-z0-9]+)?(?:\\?.*)?$")
  value_or_na(matched[, 2])
}

build_profile_url_from_ids <- function(partido_id, dni, base = "https://votoinformadoia.jne.gob.pe") {
  partido_id <- value_or_na(partido_id)
  dni <- value_or_na(dni)
  base <- coalesce_chr(base, "https://votoinformadoia.jne.gob.pe")

  if (is.na(partido_id) || is.na(dni)) {
    return(NA_character_)
  }

  use_main_route_order <- grepl("votoinformado\\.jne\\.gob\\.pe", base) &&
    !grepl("votoinformadoia\\.jne\\.gob\\.pe", base)

  if (use_main_route_order) {
    return(paste0("https://votoinformado.jne.gob.pe/hoja-vida/", partido_id, "/", dni))
  }

  paste0("https://votoinformadoia.jne.gob.pe/hoja-vida/", dni, "/", partido_id)
}

build_profile_url_from_listing <- function(url_logo_partido, dni, base = "https://votoinformadoia.jne.gob.pe") {
  partido_id <- extract_party_id_from_logo_url(url_logo_partido)
  build_profile_url_from_ids(partido_id, dni, base = base)
}

build_listing_row <- function(card_index = NA_integer_,
                              district_row = NULL,
                              numero_postulacion = NA_character_,
                              nombre = NA_character_,
                              dni_listado = NA_character_,
                              partido_politico = NA_character_,
                              cargo_postula = NA_character_,
                              type = NA_character_,
                              estado_inadmisible = NA_character_,
                              url_imagen = NA_character_,
                              url_logo_partido = NA_character_,
                              url_hoja_vida = NA_character_,
                              card_html = NA_character_) {
  tibble::tibble(
    card_index = suppressWarnings(as.integer(card_index)),
    codigo_distrito_electoral = row_value_or_na(district_row, "codigo_distrito_electoral"),
    distrito_electoral = row_value_or_na(district_row, "distrito_electoral"),
    numero_postulacion = coalesce_chr(numero_postulacion),
    nombre = coalesce_chr(nombre),
    dni_listado = coalesce_chr(dni_listado),
    partido_politico = coalesce_chr(partido_politico),
    cargo_postula = coalesce_chr(cargo_postula, row_value_or_na(district_row, "cargo_postula")),
    type = coalesce_chr(type, row_value_or_na(district_row, "type")),
    estado_inadmisible = coalesce_chr(estado_inadmisible),
    url_imagen = coalesce_chr(url_imagen),
    url_logo_partido = coalesce_chr(url_logo_partido),
    url_hoja_vida = coalesce_chr(url_hoja_vida),
    card_html = coalesce_chr(card_html)
  )
}

empty_listing_rows <- function() {
  empty_chr_tibble(listing_output_columns)
}

normalize_cargo_postula <- function(label) {
  label <- value_or_na(label)

  if (is.na(label)) {
    return(NA_character_)
  }

  cleaned <- label %>%
    stringr::str_replace(
      stringr::regex("^candidat[oa]\\s+a(?:l)?\\s+", ignore_case = TRUE),
      ""
    ) %>%
    stringr::str_squish()

  if (is.na(value_or_na(cleaned))) {
    cleaned <- label
  }

  stringr::str_to_upper(cleaned, locale = "es")
}

derive_candidate_type <- function(cargo_postula, fallback = NA_character_) {
  cargo_key <- clean_name_es(cargo_postula)

  if (!is.na(cargo_key) && grepl("vicepresident", cargo_key)) {
    return("Vicepresidente")
  }

  if (!is.na(cargo_key) && grepl("president", cargo_key)) {
    return("Presidente")
  }

  if (!is.na(cargo_key) && grepl("senador", cargo_key)) {
    return("Senador")
  }

  if (!is.na(cargo_key) && grepl("diputad", cargo_key)) {
    return("Diputado")
  }

  value_or_na(fallback)
}

extract_profile_role_label <- function(page) {
  h1_node <- rvest::html_element(page, "main h1")

  if (length(h1_node) == 0 || inherits(h1_node, "xml_missing")) {
    return(NA_character_)
  }

  containers <- list(h1_node)
  current_node <- h1_node

  for (depth_idx in seq_len(3)) {
    current_node <- xml2::xml_parent(current_node)

    if (length(current_node) == 0 || inherits(current_node, "xml_missing")) {
      break
    }

    containers[[length(containers) + 1L]] <- current_node
  }

  role_lines <- purrr::map_chr(containers, function(node) {
    p_nodes <- rvest::html_elements(node, "p")

    if (length(p_nodes) == 0) {
      return(NA_character_)
    }

    paste(compact_lines(purrr::map_chr(p_nodes, safe_html_text)), collapse = " | ")
  })

  role_lines <- compact_lines(unlist(strsplit(role_lines, "\\|", perl = TRUE), use.names = FALSE))
  role_lines <- role_lines[role_lines != safe_html_text(h1_node)]

  if (length(role_lines) == 0) {
    return(NA_character_)
  }

  role_match <- role_lines[vapply(role_lines, function(line_value) {
    line_ascii <- tolower(iconv(line_value, to = "ASCII//TRANSLIT"))
    grepl("candidat[oa]\\s+a|presidente|vicepresidente|senador|diputad", line_ascii, perl = TRUE)
  }, logical(1))]

  value_or_na(role_match[1])
}

resolve_profile_role_metadata <- function(page, listing_row = NULL) {
  cargo_postula <- coalesce_chr(
    normalize_cargo_postula(extract_profile_role_label(page)),
    normalize_cargo_postula(row_value_or_na(listing_row, "cargo_postula"))
  )

  list(
    cargo_postula = cargo_postula,
    type = coalesce_chr(
      derive_candidate_type(cargo_postula),
      row_value_or_na(listing_row, "type")
    )
  )
}

extract_listing_cards <- function(page, district_row) {
  cards <- rvest::html_elements(page, ".div-container-candidato")

  if (length(cards) == 0) {
    generic_cards <- rvest::html_elements(
      page,
      "div.block.bg-white.border.border-gray-200.rounded-lg.shadow-sm"
    )

    generic_cards <- generic_cards[
      vapply(generic_cards, function(card) {
        card_text <- safe_html_text(card)
        !is.na(card_text) && grepl("Ver hoja de vida", card_text, fixed = TRUE)
      }, logical(1))
    ]

    if (length(generic_cards) > 0) {
      return(purrr::imap_dfr(generic_cards, function(card, idx) {
        card_html <- as.character(card)
        card_lines <- compact_lines(split_multiline_text(safe_html_text(card)))

        card_lines <- card_lines[!grepl("^Ver hoja de vida$", card_lines, ignore.case = TRUE)]
        card_lines <- card_lines[!grepl("^Guardar perfil$", card_lines, ignore.case = TRUE)]

        candidate_name <- coalesce_chr(
          safe_html_text(rvest::html_element(card, "h3")),
          safe_html_text(rvest::html_element(card, "h2")),
          value_or_na(card_lines[1])
        )

        party_name <- card_lines[
          !grepl("^DNI\\s*:", card_lines, ignore.case = TRUE) &
            !grepl("^SIN ", card_lines, ignore.case = TRUE) &
            !grepl("^[0-9]+$", card_lines)
        ]

        party_name <- party_name[party_name != candidate_name]
        party_name <- coalesce_chr(
          safe_html_text(rvest::html_element(
            card,
            "div.flex.items-center.space-x-2.mb-2 span.text-sm.text-gray-600.font-medium"
          )),
          value_or_na(party_name[1])
        )

        candidate_image <- coalesce_chr(
          safe_html_attr(rvest::html_element(card, "img"), "src")
        )

        party_logo <- safe_html_attr(
          rvest::html_element(card, "div.flex.items-center.space-x-2.mb-2 img"),
          "src"
        )

        dni_listado <- value_or_na(stringr::str_match(safe_html_text(card), "DNI\\s*:?\\s*([0-9]{7,10})")[, 2])
        party_logo_url <- make_absolute_url(party_logo, base = "https://votoinformadoia.jne.gob.pe")
        profile_url <- coalesce_chr(
          extract_profile_url_from_html(card_html),
          build_profile_url_from_listing(
            party_logo_url,
            dni_listado,
            base = "https://votoinformadoia.jne.gob.pe"
          )
        )

        build_listing_row(
          card_index = idx,
          district_row = district_row,
          numero_postulacion = NA_character_,
          nombre = candidate_name,
          dni_listado = dni_listado,
          partido_politico = party_name,
          estado_inadmisible = NA_character_,
          url_imagen = make_absolute_url(candidate_image, base = "https://votoinformadoia.jne.gob.pe"),
          url_logo_partido = party_logo_url,
          url_hoja_vida = profile_url,
          card_html = card_html
        )
      }))
    }

    link_nodes <- rvest::html_elements(page, "a[href*='/hoja-vida/']")

    if (length(link_nodes) == 0) {
      return(empty_listing_rows())
    }

    link_hrefs <- purrr::map_chr(link_nodes, ~ value_or_na(rvest::html_attr(.x, "href")))
    keep_idx <- which(!duplicated(link_hrefs) & !is.na(link_hrefs))
    cards <- link_nodes[keep_idx]

    return(purrr::imap_dfr(cards, function(card, idx) {
      card_html <- as.character(card)
      card_text <- compact_lines(split_multiline_text(safe_html_text(card)))

      candidate_name <- coalesce_chr(
        safe_html_text(rvest::html_element(card, "h3")),
        safe_html_text(rvest::html_element(card, "h4")),
        safe_html_text(rvest::html_element(card, "strong")),
        safe_html_text(rvest::html_element(card, "span")),
        value_or_na(card_text[1])
      )

      party_name <- if (length(card_text) >= 2) value_or_na(card_text[length(card_text)]) else NA_character_
      candidate_image <- safe_html_attr(rvest::html_element(card, "img"), "src")

      build_listing_row(
        card_index = idx,
        district_row = district_row,
        numero_postulacion = NA_character_,
        nombre = candidate_name,
        dni_listado = value_or_na(stringr::str_match(safe_html_text(card), "DNI\\s*:?\\s*([0-9]{7,10})")[, 2]),
        partido_politico = party_name,
        estado_inadmisible = NA_character_,
        url_imagen = make_absolute_url(candidate_image, base = "https://votoinformadoia.jne.gob.pe"),
        url_logo_partido = NA_character_,
        url_hoja_vida = make_absolute_url(safe_html_attr(card, "href"), base = "https://votoinformadoia.jne.gob.pe"),
        card_html = card_html
      )
    }))
  }

  purrr::imap_dfr(cards, function(card, idx) {
    card_html <- as.character(card)

    candidate_name <- coalesce_chr(
      safe_html_text(rvest::html_element(card, "span.txt-nombre-candidato")),
      safe_html_text(rvest::html_element(card, "#divnombnrecandidato")),
      safe_html_text(rvest::html_element(card, ".content-nombre-candi"))
    )

    party_name <- coalesce_chr(
      safe_html_text(rvest::html_element(card, ".content-txt-organizacion")),
      safe_html_text(rvest::html_element(card, "#divpartidocandidadto"))
    )

    candidate_image <- coalesce_chr(
      safe_html_attr(rvest::html_element(card, "img#img-foto-candidato-pres"), "src"),
      safe_html_attr(rvest::html_element(card, "img.foto-candiadto"), "src")
    )

    party_logo <- safe_html_attr(rvest::html_element(card, "img#img-foto-org-politica"), "src")
    dni_listado <- value_or_na(stringr::str_match(safe_html_text(card), "DNI\\s*:?\\s*([0-9]{7,10})")[, 2])
    party_logo_url <- make_absolute_url(party_logo)
    profile_url <- coalesce_chr(
      extract_profile_url_from_html(card_html),
      build_profile_url_from_listing(
        party_logo_url,
        dni_listado,
        base = "https://votoinformado.jne.gob.pe"
      )
    )

    build_listing_row(
      card_index = idx,
      district_row = district_row,
      numero_postulacion = safe_html_text(rvest::html_element(card, ".numero-postulacion")),
      nombre = candidate_name,
      dni_listado = dni_listado,
      partido_politico = party_name,
      estado_inadmisible = safe_html_text(rvest::html_element(card, "#candidato-inadminisible-estado")),
      url_imagen = make_absolute_url(candidate_image),
      url_logo_partido = party_logo_url,
      url_hoja_vida = profile_url,
      card_html = card_html
    )
  })
}

extract_formula_candidate_cards <- function(page, district_row) {
  cards <- rvest::html_elements(
    page,
    "div.bg-white.border.border-gray-200.rounded-lg.p-4.shadow-sm.w-64.text-center.cursor-pointer"
  )

  if (length(cards) == 0) {
    return(empty_listing_rows())
  }

  heading_candidates <- compact_lines(c(
    safe_html_text(rvest::html_element(page, "main h1")),
    safe_html_text(rvest::html_element(page, "main h2"))
  ))
  heading_candidates <- heading_candidates[
    !grepl("formula_presidencial|resumen_de_plan_de_gobierno", clean_name_es(heading_candidates))
  ]
  party_name <- value_or_na(heading_candidates[1])

  purrr::imap_dfr(cards, function(card, idx) {
    card_html <- as.character(card)
    cargo_postula <- normalize_cargo_postula(safe_html_text(rvest::html_element(card, "p")))

    build_listing_row(
      card_index = idx,
      district_row = district_row,
      numero_postulacion = coalesce_chr(
        safe_html_text(rvest::html_element(card, ".numero-postulacion-form")),
        as.character(idx)
      ),
      nombre = safe_html_text(rvest::html_element(card, "h3")),
      dni_listado = NA_character_,
      partido_politico = party_name,
      cargo_postula = cargo_postula,
      type = derive_candidate_type(cargo_postula),
      estado_inadmisible = NA_character_,
      url_imagen = make_absolute_url(safe_html_attr(rvest::html_element(card, "img"), "src")),
      url_logo_partido = NA_character_,
      url_hoja_vida = NA_character_,
      card_html = card_html
    )
  })
}

extract_label_value_rows <- function(container_node) {
  rows <- rvest::html_elements(container_node, "div.flex.items-start")

  if (length(rows) == 0) {
    return(tibble::tibble(label = character(), value = character()))
  }

  purrr::map_dfr(rows, function(row_node) {
    label <- safe_html_text(rvest::html_element(row_node, "span"))
    value <- coalesce_chr(
      safe_html_text(rvest::html_element(row_node, "p")),
      {
        spans <- rvest::html_elements(row_node, "span")

        if (length(spans) >= 2) {
          safe_html_text(spans[[length(spans)]])
        } else {
          NA_character_
        }
      }
    )

    tibble::tibble(
      label = label,
      value = value
    )
  }) %>%
    dplyr::filter(!is.na(.data$label), !is.na(.data$value))
}

extract_profile_key_values <- function(page) {
  sections <- extract_section_nodes(page)
  general_section <- sections[["informacion_general"]]
  summary_node <- extract_profile_summary_node(page)

  summary_kv <- if (length(summary_node) == 0 || inherits(summary_node, "xml_missing")) {
    tibble::tibble(label = character(), value = character(), label_clean = character())
  } else {
    summary_rows <- rvest::html_elements(summary_node, "div.grid div.flex.gap-2")

    if (length(summary_rows) == 0) {
      tibble::tibble(label = character(), value = character(), label_clean = character())
    } else {
      purrr::map_dfr(summary_rows, function(row_node) {
        spans <- compact_lines(purrr::map_chr(rvest::html_elements(row_node, "span"), safe_html_text))

        if (length(spans) < 2) {
          return(tibble::tibble(
            label = character(),
            value = character(),
            label_clean = character()
          ))
        }

        label_value <- value_or_na(spans[1])
        value_value <- value_or_na(spans[length(spans)])

        if (is.na(label_value) || is.na(value_value)) {
          return(tibble::tibble(
            label = character(),
            value = character(),
            label_clean = character()
          ))
        }

        tibble::tibble(
          label = label_value,
          value = value_value,
          label_clean = clean_name_es(label_value)
        )
      })
    }
  }

  if (is.null(general_section)) {
    return(summary_kv)
  }

  kv_table <- extract_label_value_rows(general_section$node)

  if (nrow(kv_table) == 0) {
    return(summary_kv)
  }

  dplyr::bind_rows(
    kv_table %>%
      dplyr::mutate(label_clean = clean_name_es(.data$label)),
    summary_kv
  ) %>%
    dplyr::filter(!is.na(.data$label_clean)) %>%
    dplyr::distinct(.data$label_clean, .keep_all = TRUE)
}

extract_section_nodes <- function(page) {
  section_entries <- list()
  summary_node <- extract_profile_summary_node(page)

  if (length(summary_node) > 0 && !inherits(summary_node, "xml_missing")) {
    section_entries[[length(section_entries) + 1L]] <- list(
      title = "Informacion General",
      key = "informacion_general",
      node = summary_node
    )
  }

  legacy_sections <- rvest::html_elements(page, "main section")

  if (length(legacy_sections) > 0) {
    section_entries <- c(
      section_entries,
      purrr::map(legacy_sections, function(section_node) {
        title <- safe_html_text(rvest::html_element(section_node, "h2"))

        list(
          title = title,
          key = section_title_key(title),
          node = section_node
        )
      })
    )
  }

  modern_sections <- rvest::html_elements(
    page,
    "main div.space-y-4 > div.bg-white.border.border-gray-200.rounded-lg.shadow-sm.overflow-hidden"
  )

  if (length(modern_sections) > 0) {
    section_entries <- c(
      section_entries,
      purrr::map(modern_sections, function(section_node) {
        title <- safe_html_text(rvest::html_element(section_node, "button"))

        list(
          title = title,
          key = section_title_key(title),
          node = section_node
        )
      })
    )
  }

  if (length(section_entries) == 0) {
    return(list())
  }

  keys <- purrr::map_chr(section_entries, "key")
  valid_idx <- !is.na(keys) & nzchar(keys)

  section_entries <- section_entries[valid_idx]
  keys <- keys[valid_idx]

  stats::setNames(section_entries, keys)
}

section_has_no_info <- function(section_node) {
  section_text <- safe_html_text(section_node)

  if (is.na(section_text)) {
    return(FALSE)
  }

  section_ascii <- iconv(section_text, to = "ASCII//TRANSLIT")
  grepl("No registra|No tiene", section_ascii, ignore.case = TRUE)
}

build_detail_value <- function(values) {
  values <- compact_lines(values)

  if (length(values) == 0) {
    return(NA_character_)
  }

  paste(values, collapse = " | ")
}

extract_table_header_keys <- function(table_node) {
  headers <- compact_lines(purrr::map_chr(rvest::html_elements(table_node, "thead th"), safe_html_text))
  clean_name_es(headers)
}

extract_table_cell_value <- function(cells, header_keys, patterns, default_index = NA_integer_) {
  cells <- as.character(cells)
  header_keys <- as.character(header_keys)

  if (length(header_keys) > 0) {
    match_idx <- which(vapply(header_keys, function(header_key) {
      any(vapply(patterns, function(pattern_value) {
        grepl(pattern_value, header_key, perl = TRUE)
      }, logical(1)))
    }, logical(1)))

    if (length(match_idx) > 0 && length(cells) >= match_idx[[1]]) {
      return(value_or_na(cells[match_idx[[1]]]))
    }
  }

  if (!is.na(default_index) && length(cells) >= default_index) {
    return(value_or_na(cells[default_index]))
  }

  NA_character_
}

extract_heading_blocks <- function(section_node) {
  heading_nodes <- rvest::html_elements(section_node, "h4")

  if (length(heading_nodes) > 0) {
    blocks <- purrr::map(heading_nodes, function(heading_node) {
      title <- safe_html_text(heading_node)

      list(
        title = title,
        key = clean_name_es(title),
        node = xml2::xml_parent(heading_node)
      )
    })

    return(stats::setNames(blocks, purrr::map_chr(blocks, "key")))
  }

  subsection_nodes <- rvest::html_elements(section_node, "div.p-0-1 > div")

  if (length(subsection_nodes) == 0) {
    return(list())
  }

  blocks <- purrr::map(subsection_nodes, function(subsection_node) {
    title <- coalesce_chr(
      safe_html_text(rvest::html_element(subsection_node, "#content-cargos-partidarios")),
      safe_html_text(rvest::html_element(subsection_node, "#content-eleccion-popular")),
      safe_html_text(rvest::html_element(subsection_node, "div.w-full.bg-gray-400.text-white.text-center"))
    )

    list(
      title = title,
      key = section_title_key(title),
      node = subsection_node
    )
  })

  keys <- purrr::map_chr(blocks, "key")
  valid_idx <- !is.na(keys) & nzchar(keys)

  blocks <- blocks[valid_idx]
  keys <- keys[valid_idx]

  stats::setNames(blocks, keys)
}

find_heading_block <- function(section_node, patterns) {
  blocks <- extract_heading_blocks(section_node)

  if (length(blocks) == 0) {
    return(NULL)
  }

  keys <- names(blocks)
  matched_idx <- which(vapply(keys, function(block_key) {
    any(vapply(patterns, function(pattern_value) {
      grepl(pattern_value, block_key, perl = TRUE)
    }, logical(1)))
  }, logical(1)))

  if (length(matched_idx) == 0) {
    return(NULL)
  }

  blocks[[matched_idx[[1]]]]
}

extract_education_basic_values <- function(section_node) {
  out <- list(
    cuenta = NA_character_,
    primaria = NA_character_,
    secundaria = NA_character_
  )

  section_text <- safe_html_text(section_node)

  if (is.na(section_text)) {
    return(out)
  }

  row_nodes <- rvest::html_elements(section_node, "div.flex.justify-between")

  if (length(row_nodes) > 0) {
    parsed_rows <- purrr::map_dfr(row_nodes, function(row_node) {
      spans <- compact_lines(purrr::map_chr(rvest::html_elements(row_node, "span"), safe_html_text))

      if (length(spans) < 2) {
        return(tibble::tibble(label = character(), value = character()))
      }

      tibble::tibble(
        label = value_or_na(spans[1]),
        value = value_or_na(spans[length(spans)])
      )
    })

    if (nrow(parsed_rows) > 0) {
      label_keys <- clean_name_es(parsed_rows$label)
      out$cuenta <- value_or_na(parsed_rows$value[grepl("educacion_basica", label_keys)][1])
      out$primaria <- value_or_na(parsed_rows$value[grepl("^primaria$", label_keys)][1])
      out$secundaria <- value_or_na(parsed_rows$value[grepl("^secundaria$", label_keys)][1])
    }
  }

  if (!is.na(out$cuenta)) {
    return(out)
  }

  section_ascii <- iconv(section_text, to = "ASCII//TRANSLIT")
  matched <- stringr::str_match(section_ascii, "Educacion Basica\\s*:?\\s*(SI|NO)")
  out$cuenta <- value_or_na(matched[, 2])
  out
}

extract_education_basic_value <- function(section_node) {
  extract_education_basic_values(section_node)$cuenta
}

empty_study_rows <- function() {
  empty_chr_tibble(section_default_columns$estudios_universitarios)
}

parse_study_card_row <- function(card_node, id_candidato) {
  p_texts <- compact_lines(purrr::map_chr(rvest::html_elements(card_node, "p"), safe_html_text))
  badge_texts <- compact_lines(purrr::map_chr(rvest::html_elements(card_node, "span"), safe_html_text))
  badge_ascii <- iconv(badge_texts, to = "ASCII//TRANSLIT")
  year_candidates <- badge_ascii[grepl("^Ano\\s*:", badge_ascii)]
  year_value <- if (length(year_candidates) > 0) {
    value_or_na(stringr::str_match(year_candidates[[1]], "Ano\\s*:?\\s*([0-9]{4})")[, 2])
  } else {
    NA_character_
  }

  badge_texts_no_year <- badge_texts[!grepl("^Ano\\s*:", badge_ascii)]
  badge_keys <- clean_name_es(badge_texts_no_year)
  level_candidates <- c("maestria", "doctorado", "doctor", "magister", "master", "especializacion", "segunda_especialidad", "diplomado")
  level_idx <- which(badge_keys %in% level_candidates)
  grado_value <- if (length(level_idx) > 0) value_or_na(badge_texts_no_year[level_idx[[1]]]) else NA_character_
  condicion_badges <- if (length(level_idx) > 0) badge_texts_no_year[-level_idx[[1]]] else badge_texts_no_year
  study_value <- value_or_na(p_texts[1])
  institution_value <- value_or_na(p_texts[2])
  condition_value <- build_detail_value(condicion_badges)
  detail_value <- build_detail_value(c(grado_value, study_value, institution_value, condicion_badges, if (!is.na(year_value)) paste("Ano", year_value)))

  if (is.na(detail_value)) {
    return(empty_study_rows())
  }

  tibble::tibble(
    id_candidato = id_candidato,
    estudio = study_value,
    institucion = institution_value,
    grado = grado_value,
    condicion = condition_value,
    anio = year_value,
    detalle_original = detail_value
  )
}

extract_study_list_rows <- function(block_node, id_candidato) {
  li_nodes <- rvest::html_elements(block_node, "li")

  if (length(li_nodes) == 0) {
    return(empty_study_rows())
  }

  purrr::map_dfr(li_nodes, function(li_node) {
    spans <- compact_lines(purrr::map_chr(rvest::html_elements(li_node, "span"), safe_html_text))
    spans <- spans[spans != "-"]
    li_text <- build_detail_value(spans)

    if (is.na(li_text)) {
      li_text <- safe_html_text(li_node)
    }

    li_ascii <- iconv(coalesce_chr(li_text), to = "ASCII//TRANSLIT")

    if (is.na(li_text) || !nzchar(li_text) || grepl("^-$", li_text) || grepl("^No registra", li_ascii, ignore.case = TRUE)) {
      return(empty_study_rows())
    }

    study_value <- NA_character_
    institution_value <- NA_character_

    if (length(spans) >= 2) {
      study_value <- value_or_na(spans[1])
      institution_value <- value_or_na(paste(spans[-1], collapse = " - "))
    } else {
      parts <- strsplit(li_text, " - ", fixed = TRUE)[[1]]

      if (length(parts) >= 2) {
        study_value <- value_or_na(parts[1])
        institution_value <- value_or_na(paste(parts[-1], collapse = " - "))
      } else {
        study_value <- li_text
      }
    }

    tibble::tibble(
      id_candidato = id_candidato,
      estudio = study_value,
      institucion = institution_value,
      grado = NA_character_,
      condicion = NA_character_,
      anio = NA_character_,
      detalle_original = build_detail_value(c(study_value, institution_value))
    )
  }) %>%
    dplyr::filter(!is.na(.data$detalle_original))
}

extract_study_rows_from_block <- function(block_node, id_candidato) {
  if (is.null(block_node) || inherits(block_node, "xml_missing") || section_has_no_info(block_node)) {
    return(empty_study_rows())
  }

  table_node <- rvest::html_element(block_node, "table")

  if (length(table_node) > 0 && !inherits(table_node, "xml_missing")) {
    header_keys <- extract_table_header_keys(table_node)
    row_nodes <- rvest::html_elements(table_node, "tbody tr")

    if (length(row_nodes) > 0) {
      parsed_rows <- purrr::map_dfr(row_nodes, function(row_node) {
        cells <- compact_lines(purrr::map_chr(rvest::html_elements(row_node, "th, td"), safe_html_text))
        detail_value <- build_detail_value(cells)

        if (is.na(detail_value)) {
          return(empty_study_rows())
        }

        tibble::tibble(
          id_candidato = id_candidato,
          estudio = extract_table_cell_value(
            cells,
            header_keys,
            c("^especialidad$", "^estudio$", "^carrera$", "^profesion$"),
            default_index = if (length(cells) >= 5) 2L else NA_integer_
          ),
          institucion = extract_table_cell_value(
            cells,
            header_keys,
            c("^universidad$", "^institucion$", "^centro_de_estudio$", "^centro_de_formacion$"),
            default_index = 1L
          ),
          grado = extract_table_cell_value(
            cells,
            header_keys,
            c("^grado$"),
            default_index = if (length(cells) >= 4) 3L else NA_integer_
          ),
          condicion = extract_table_cell_value(
            cells,
            header_keys,
            c("^concluido$", "^condicion$"),
            default_index = if (length(cells) >= 4) 2L else NA_integer_
          ),
          anio = extract_table_cell_value(
            cells,
            header_keys,
            c("^ano_de_obtencion$", "^ano$"),
            default_index = if (length(cells) >= 4) 4L else NA_integer_
          ),
          detalle_original = detail_value
        )
      }) %>%
        dplyr::filter(!is.na(.data$detalle_original))

      if (nrow(parsed_rows) > 0) {
        return(parsed_rows)
      }
    }
  }

  card_nodes <- rvest::html_elements(block_node, "div.bg-gray-50.p-4.rounded-lg.border")

  if (length(card_nodes) > 0) {
    parsed_cards <- purrr::map_dfr(card_nodes, parse_study_card_row, id_candidato = id_candidato) %>%
      dplyr::filter(!is.na(.data$detalle_original))

    if (nrow(parsed_cards) > 0) {
      return(parsed_cards)
    }
  }

  extract_study_list_rows(block_node, id_candidato)
}

empty_experience_rows <- function() {
  empty_chr_tibble(section_default_columns$experiencia_laboral)
}

extract_experience_rows <- function(section_node, id_candidato) {
  if (is.null(section_node) || inherits(section_node, "xml_missing") || section_has_no_info(section_node)) {
    return(empty_experience_rows())
  }

  table_node <- rvest::html_element(section_node, "table")

  if (length(table_node) > 0 && !inherits(table_node, "xml_missing")) {
    header_keys <- extract_table_header_keys(table_node)
    row_nodes <- rvest::html_elements(table_node, "tbody tr")

    if (length(row_nodes) > 0) {
      parsed_rows <- purrr::map_dfr(row_nodes, function(row_node) {
        cells <- compact_lines(purrr::map_chr(rvest::html_elements(row_node, "th, td"), safe_html_text))
        detail_value <- build_detail_value(cells)

        if (is.na(detail_value)) {
          return(empty_experience_rows())
        }

        tibble::tibble(
          id_candidato = id_candidato,
          entidad = extract_table_cell_value(
            cells,
            header_keys,
            c("^centro_de_trabajo$", "^entidad$", "^institucion$"),
            default_index = 1L
          ),
          cargo = extract_table_cell_value(
            cells,
            header_keys,
            c("^ocupacion$", "^cargo$"),
            default_index = 2L
          ),
          periodo = extract_table_cell_value(
            cells,
            header_keys,
            c("^periodo$"),
            default_index = 3L
          ),
          detalle_original = detail_value
        )
      }) %>%
        dplyr::filter(!is.na(.data$detalle_original))

      if (nrow(parsed_rows) > 0) {
        return(parsed_rows)
      }
    }
  }

  row_nodes <- rvest::html_elements(section_node, "div.flex.items-start")

  if (length(row_nodes) == 0) {
    return(empty_experience_rows())
  }

  purrr::map_dfr(row_nodes, function(row_node) {
    p_texts <- compact_lines(purrr::map_chr(rvest::html_elements(row_node, "p"), safe_html_text))
    detail_value <- build_detail_value(p_texts)

    if (is.na(detail_value)) {
      return(empty_experience_rows())
    }

    tibble::tibble(
      id_candidato = id_candidato,
      entidad = value_or_na(p_texts[1]),
      cargo = value_or_na(p_texts[2]),
      periodo = value_or_na(p_texts[3]),
      detalle_original = detail_value
    )
  }) %>%
    dplyr::filter(!is.na(.data$detalle_original))
}

empty_trayectoria_rows <- function() {
  empty_chr_tibble(section_default_columns$cargos_partidarios)
}

extract_trayectoria_rows_from_block <- function(block_node, id_candidato) {
  if (is.null(block_node) || inherits(block_node, "xml_missing") || section_has_no_info(block_node)) {
    return(empty_trayectoria_rows())
  }

  table_node <- rvest::html_element(block_node, "table")

  if (length(table_node) > 0 && !inherits(table_node, "xml_missing")) {
    header_keys <- extract_table_header_keys(table_node)
    row_nodes <- rvest::html_elements(table_node, "tbody tr")

    if (length(row_nodes) > 0) {
      parsed_rows <- purrr::map_dfr(row_nodes, function(row_node) {
        cells <- compact_lines(purrr::map_chr(rvest::html_elements(row_node, "th, td"), safe_html_text))
        detail_value <- build_detail_value(cells)

        if (is.na(detail_value)) {
          return(empty_trayectoria_rows())
        }

        tibble::tibble(
          id_candidato = id_candidato,
          cargo = extract_table_cell_value(
            cells,
            header_keys,
            c("^cargo$"),
            default_index = if (length(cells) >= 2) 2L else 1L
          ),
          organizacion = extract_table_cell_value(
            cells,
            header_keys,
            c("^organizacion_politica$", "^organizacion$"),
            default_index = 1L
          ),
          periodo = extract_table_cell_value(
            cells,
            header_keys,
            c("^periodo$"),
            default_index = 3L
          ),
          detalle_original = detail_value
        )
      }) %>%
        dplyr::filter(!is.na(.data$detalle_original))

      if (nrow(parsed_rows) > 0) {
        return(parsed_rows)
      }
    }
  }

  card_nodes <- rvest::html_elements(block_node, "div.bg-gray-50.p-4.rounded-lg.border")

  if (length(card_nodes) == 0) {
    return(empty_trayectoria_rows())
  }

  purrr::map_dfr(card_nodes, function(card_node) {
    p_texts <- compact_lines(purrr::map_chr(rvest::html_elements(card_node, "p"), safe_html_text))
    period_value <- coalesce_chr(safe_html_text(rvest::html_element(card_node, ".whitespace-nowrap")), value_or_na(p_texts[3]))
    detail_value <- build_detail_value(c(value_or_na(p_texts[1]), value_or_na(p_texts[2]), period_value))

    if (is.na(detail_value)) {
      return(empty_trayectoria_rows())
    }

    tibble::tibble(
      id_candidato = id_candidato,
      cargo = value_or_na(p_texts[1]),
      organizacion = value_or_na(p_texts[2]),
      periodo = period_value,
      detalle_original = detail_value
    )
  }) %>%
    dplyr::filter(!is.na(.data$detalle_original))
}

empty_sentence_rows <- function() {
  empty_chr_tibble(section_default_columns$relacion_sentencias)
}

extract_sentence_rows <- function(section_node, id_candidato) {
  if (is.null(section_node) || inherits(section_node, "xml_missing") || section_has_no_info(section_node)) {
    return(empty_sentence_rows())
  }

  table_node <- rvest::html_element(section_node, "table")

  if (length(table_node) > 0 && !inherits(table_node, "xml_missing")) {
    row_nodes <- rvest::html_elements(table_node, "tbody tr")

    if (length(row_nodes) > 0) {
      parsed_rows <- purrr::map_dfr(row_nodes, function(row_node) {
        cells <- compact_lines(purrr::map_chr(rvest::html_elements(row_node, "th, td"), safe_html_text))
        detail_value <- build_detail_value(cells)

        if (is.na(detail_value)) {
          return(empty_sentence_rows())
        }

        tibble::tibble(
          id_candidato = id_candidato,
          tipo_sentencia = NA_character_,
          materia = value_or_na(cells[1]),
          resultado = value_or_na(cells[2]),
          detalle_original = detail_value
        )
      }) %>%
        dplyr::filter(!is.na(.data$detalle_original))

      if (nrow(parsed_rows) > 0) {
        return(parsed_rows)
      }
    }
  }

  blocks <- extract_heading_blocks(section_node)

  if (length(blocks) == 0) {
    return(empty_sentence_rows())
  }

  purrr::map_dfr(blocks, function(block) {
    tipo_sentencia <- stringr::str_replace(coalesce_chr(block$title), "\\s*\\([0-9]+\\)$", "")
    card_nodes <- rvest::html_elements(block$node, "div.border-l-4")

    if (length(card_nodes) == 0) {
      return(empty_sentence_rows())
    }

    purrr::map_dfr(card_nodes, function(card_node) {
      p_texts <- compact_lines(purrr::map_chr(rvest::html_elements(card_node, "p"), safe_html_text))
      detail_value <- build_detail_value(c(tipo_sentencia, p_texts))

      if (is.na(detail_value)) {
        return(empty_sentence_rows())
      }

      tibble::tibble(
        id_candidato = id_candidato,
        tipo_sentencia = tipo_sentencia,
        materia = value_or_na(p_texts[1]),
        resultado = value_or_na(p_texts[2]),
        detalle_original = detail_value
      )
    })
  }) %>%
    dplyr::filter(!is.na(.data$detalle_original))
}

extract_income_values <- function(section_node) {
  empty_values <- list(
    remuneracion_bruta_publico = NA_character_,
    remuneracion_bruta_privado = NA_character_,
    total_ingresos = NA_character_
  )

  if (is.null(section_node) || inherits(section_node, "xml_missing")) {
    return(empty_values)
  }

  table_node <- rvest::html_element(section_node, "table")

  parsed_rows <- tibble::tibble(
    descripcion = character(),
    sector_publico = character(),
    sector_privado = character(),
    total = character()
  )

  if (length(table_node) > 0 && !inherits(table_node, "xml_missing")) {
    row_nodes <- rvest::html_elements(table_node, "tbody tr")

    if (length(row_nodes) > 0) {
      parsed_rows <- purrr::map_dfr(row_nodes, function(row_node) {
        cells <- rvest::html_elements(row_node, "th, td")
        cell_texts <- purrr::map_chr(cells, safe_html_text)

        if (length(cell_texts) < 4) {
          return(tibble::tibble(
            descripcion = character(),
            sector_publico = character(),
            sector_privado = character(),
            total = character()
          ))
        }

        tibble::tibble(
          descripcion = value_or_na(cell_texts[1]),
          sector_publico = value_or_na(cell_texts[2]),
          sector_privado = value_or_na(cell_texts[3]),
          total = value_or_na(cell_texts[4])
        )
      })
    }
  }

  if (nrow(parsed_rows) == 0) {
    row_nodes <- rvest::html_elements(section_node, "div.grid.grid-cols-2")

    if (length(row_nodes) == 0) {
      return(empty_values)
    }

    simple_rows <- purrr::map_dfr(row_nodes, function(row_node) {
      spans <- compact_lines(purrr::map_chr(rvest::html_elements(row_node, "span"), safe_html_text))

      if (length(spans) < 2) {
        return(tibble::tibble(descripcion = character(), valor = character()))
      }

      tibble::tibble(
        descripcion = value_or_na(spans[1]),
        valor = value_or_na(spans[length(spans)])
      )
    })

    if (nrow(simple_rows) == 0) {
      return(empty_values)
    }

    descripcion_ascii <- iconv(simple_rows$descripcion, to = "ASCII//TRANSLIT")

    return(list(
      remuneracion_bruta_publico = value_or_na(simple_rows$valor[grepl("Remuneracion Bruta Publico", descripcion_ascii, ignore.case = TRUE)][1]),
      remuneracion_bruta_privado = value_or_na(simple_rows$valor[grepl("Remuneracion Bruta Privado", descripcion_ascii, ignore.case = TRUE)][1]),
      total_ingresos = value_or_na(simple_rows$valor[grepl("Total Ingresos", descripcion_ascii, ignore.case = TRUE)][1])
    ))
  }

  descripcion_ascii <- iconv(parsed_rows$descripcion, to = "ASCII//TRANSLIT")
  remuneracion_row <- parsed_rows[grepl("^REMUNERACION BRUTA ANUAL", descripcion_ascii), , drop = FALSE]
  total_row <- parsed_rows[grepl("^TOTAL DE INGRESOS", descripcion_ascii), , drop = FALSE]

  list(
    remuneracion_bruta_publico = value_or_na(remuneracion_row$sector_publico),
    remuneracion_bruta_privado = value_or_na(remuneracion_row$sector_privado),
    total_ingresos = value_or_na(total_row$total)
  )
}

empty_asset_rows <- function() {
  empty_chr_tibble(section_default_columns$bienes_muebles_inmuebles)
}

extract_asset_rows <- function(section_node, id_candidato) {
  if (is.null(section_node) || inherits(section_node, "xml_missing") || section_has_no_info(section_node)) {
    return(empty_asset_rows())
  }

  table_node <- rvest::html_element(section_node, "table")

  if (length(table_node) > 0 && !inherits(table_node, "xml_missing")) {
    row_nodes <- rvest::html_elements(table_node, "tbody tr")

    if (length(row_nodes) > 0) {
      parsed_rows <- purrr::map_dfr(row_nodes, function(row_node) {
        cells <- compact_lines(purrr::map_chr(rvest::html_elements(row_node, "th, td"), safe_html_text))
        detail_value <- build_detail_value(cells)

        if (is.na(detail_value)) {
          return(empty_asset_rows())
        }

        tibble::tibble(
          id_candidato = id_candidato,
          tipo_bien = value_or_na(cells[1]),
          descripcion = value_or_na(cells[2]),
          detalle = NA_character_,
          valor = value_or_na(cells[3]),
          detalle_original = detail_value
        )
      }) %>%
        dplyr::filter(!is.na(.data$detalle_original))

      if (nrow(parsed_rows) > 0) {
        return(parsed_rows)
      }
    }
  }

  blocks <- extract_heading_blocks(section_node)

  if (length(blocks) == 0) {
    return(empty_asset_rows())
  }

  purrr::map_dfr(blocks, function(block) {
    block_ascii <- iconv(coalesce_chr(safe_html_text(block$node)), to = "ASCII//TRANSLIT")

    if (grepl("^No registra bienes", block_ascii, ignore.case = TRUE)) {
      return(empty_asset_rows())
    }

    li_nodes <- rvest::html_elements(block$node, "li")

    if (length(li_nodes) == 0) {
      return(empty_asset_rows())
    }

    purrr::map_dfr(li_nodes, function(li_node) {
      p_texts <- compact_lines(purrr::map_chr(rvest::html_elements(li_node, "p"), safe_html_text))
      value_label <- safe_html_text(rvest::html_element(li_node, "span.text-xs"))
      value_value <- coalesce_chr(
        safe_html_text(rvest::html_element(li_node, "span.font-bold")),
        {
          span_texts <- compact_lines(purrr::map_chr(rvest::html_elements(li_node, "span"), safe_html_text))
          value_or_na(tail(span_texts, 1))
        }
      )
      detail_value <- build_detail_value(c(block$title, p_texts, value_label, value_value))

      if (is.na(detail_value)) {
        return(empty_asset_rows())
      }

      tibble::tibble(
        id_candidato = id_candidato,
        tipo_bien = block$title,
        descripcion = value_or_na(p_texts[1]),
        detalle = value_or_na(p_texts[2]),
        valor = value_value,
        detalle_original = detail_value
      )
    })
  }) %>%
    dplyr::filter(!is.na(.data$detalle_original))
}

extract_additional_info <- function(section_node, id_candidato) {
  if (is.null(section_node) || inherits(section_node, "xml_missing") || section_has_no_info(section_node)) {
    return(tibble::tibble(
      id_candidato = character(),
      titulo = character(),
      detalle = character(),
      detalle_original = character()
    ))
  }

  info_blocks <- rvest::html_elements(section_node, "div.border-l-4, div.bg-gray-50.p-4.rounded-lg")

  if (length(info_blocks) > 0) {
    parsed_blocks <- purrr::map_dfr(info_blocks, function(info_node) {
      title_value <- safe_html_text(rvest::html_element(info_node, "h4"))
      detail_lines <- compact_lines(purrr::map_chr(rvest::html_elements(info_node, "p, li"), safe_html_text))
      detail_ascii <- iconv(detail_lines, to = "ASCII//TRANSLIT")
      detail_lines <- detail_lines[!grepl("^No registra informacion adicional", detail_ascii, ignore.case = TRUE)]
      detail_value <- build_detail_value(detail_lines)

      if (is.na(detail_value)) {
        return(tibble::tibble(
          id_candidato = character(),
          titulo = character(),
          detalle = character(),
          detalle_original = character()
        ))
      }

      tibble::tibble(
        id_candidato = id_candidato,
        titulo = title_value,
        detalle = detail_value,
        detalle_original = build_detail_value(c(title_value, detail_value))
      )
    }) %>%
      dplyr::filter(!is.na(.data$detalle_original))

    if (nrow(parsed_blocks) > 0) {
      return(parsed_blocks)
    }
  }

  text_lines <- compact_lines(split_multiline_text(safe_html_text(section_node)))
  text_ascii <- iconv(text_lines, to = "ASCII//TRANSLIT")
  text_lines <- text_lines[
    !grepl("^Informacion Adicional$", text_ascii, ignore.case = TRUE) &
      !grepl("^No registra informacion adicional", text_ascii, ignore.case = TRUE)
  ]

  if (length(text_lines) == 0) {
    return(tibble::tibble(
      id_candidato = character(),
      titulo = character(),
      detalle = character(),
      detalle_original = character()
    ))
  }

  tibble::tibble(
    id_candidato = id_candidato,
    titulo = NA_character_,
    detalle = text_lines,
    detalle_original = text_lines
  )
}

extract_annotation_rows <- function(section_node, id_candidato) {
  text_lines <- compact_lines(split_multiline_text(safe_html_text(section_node)))
  text_lines <- text_lines[!grepl("^ANOTACION MARGINAL$", clean_name_es(text_lines))]
  text_lines <- text_lines[!grepl("^No registra informacion\\.?$", iconv(text_lines, to = "ASCII//TRANSLIT"), ignore.case = TRUE)]

  if (length(text_lines) == 0) {
    return(tibble::tibble(
      id_candidato = character(),
      detalle = character(),
      detalle_original = character()
    ))
  }

  tibble::tibble(
    id_candidato = id_candidato,
    detalle = text_lines,
    detalle_original = text_lines
  )
}

extract_profile_photo_url <- function(page) {
  candidate_img <- coalesce_chr(
    safe_html_attr(rvest::html_element(page, "main img[src*='mpesije.jne.gob.pe']"), "src"),
    safe_html_attr(rvest::html_element(page, "main img[src*='/apidocs/']"), "src"),
    safe_html_attr(rvest::html_element(page, "main img.rounded-full.object-cover"), "src")
  )
  make_absolute_url(candidate_img)
}

extract_profile_logo_url <- function(page) {
  summary_node <- extract_profile_summary_node(page)
  logo_img <- coalesce_chr(
    safe_html_attr(rvest::html_element(summary_node, "img[src*='LogoOp/']"), "src"),
    safe_html_attr(rvest::html_element(page, "main img[alt='Logo partido']"), "src"),
    safe_html_attr(rvest::html_element(page, "main img[alt='Logo Partido']"), "src")
  )
  make_absolute_url(logo_img)
}

build_candidate_record_id <- function(partido_id, dni, type = NA_character_, target_slug = NA_character_) {
  partido_id <- value_or_na(partido_id)
  dni <- value_or_na(dni)

  if (is.na(partido_id) || is.na(dni)) {
    return(NA_character_)
  }

  type_slug <- slugify_path(type)

  if (!is.na(type_slug) && nzchar(type_slug)) {
    return(paste(partido_id, dni, type_slug, sep = "_"))
  }

  target_slug <- slugify_path(target_slug)

  if (!is.na(target_slug) && nzchar(target_slug)) {
    return(paste(partido_id, dni, target_slug, sep = "_"))
  }

  paste(partido_id, dni, sep = "_")
}

derive_candidate_ids <- function(url_hoja_vida, type = NA_character_, target_slug = NA_character_) {
  matched <- stringr::str_match(url_hoja_vida, "/hoja-vida/([0-9]+)/([0-9]+)")

  if (nrow(matched) == 0 || all(is.na(matched[1, ]))) {
    return(list(
      partido_id = NA_character_,
      dni = NA_character_,
      person_id = NA_character_,
      id_candidato = NA_character_
    ))
  }

  first_id <- matched[1, 2]
  second_id <- matched[1, 3]

  if (nchar(first_id) >= 8 && nchar(second_id) < 8) {
    dni <- first_id
    partido_id <- second_id
  } else if (nchar(second_id) >= 8 && nchar(first_id) < 8) {
    dni <- second_id
    partido_id <- first_id
  } else {
    partido_id <- first_id
    dni <- second_id
  }

  list(
    partido_id = partido_id,
    dni = dni,
    person_id = paste(partido_id, dni, sep = "_"),
    id_candidato = build_candidate_record_id(
      partido_id,
      dni,
      type = type,
      target_slug = target_slug
    )
  )
}

parse_candidate_profile <- function(page, listing_row, district_row, url_hoja_vida) {
  ids <- derive_candidate_ids(
    url_hoja_vida,
    type = row_value_or_na(listing_row, "type"),
    target_slug = row_value_or_na(district_row, "target_slug")
  )
  profile_kv <- extract_profile_key_values(page)
  sections <- extract_section_nodes(page)
  role_metadata <- resolve_profile_role_metadata(page, listing_row)

  top_lookup <- as.list(stats::setNames(profile_kv$value, profile_kv$label_clean))
  income_values <- list(
    remuneracion_bruta_publico = NA_character_,
    remuneracion_bruta_privado = NA_character_,
    total_ingresos = NA_character_
  )

  education_section <- sections[["formacion_academica"]]
  basic_section <- sections[["educacion_basica"]]
  income_section <- sections[["ingreso_de_bienes_y_rentas"]]
  trajectory_section <- sections[["trayectoria_politica"]]
  sentences_section <- sections[["declaracion_de_sentencias_firmes"]]
  assets_section <- sections[["bienes_muebles_inmuebles"]]
  additional_section <- sections[["informacion_adicional"]]
  annotation_section <- sections[["anotacion_marginal"]]

  basic_values <- if (!is.null(basic_section)) {
    extract_education_basic_values(basic_section$node)
  } else if (!is.null(education_section)) {
    extract_education_basic_values(education_section$node)
  } else {
    list(
      cuenta = NA_character_,
      primaria = NA_character_,
      secundaria = NA_character_
    )
  }

  if (!is.null(income_section)) {
    income_values <- extract_income_values(income_section$node)
  }

  profile_image_url <- extract_profile_photo_url(page)
  image_url <- coalesce_chr(
    if (!is_placeholder_image(profile_image_url)) profile_image_url else NA_character_,
    if (!is_placeholder_image(row_value_or_na(listing_row, "url_imagen"))) row_value_or_na(listing_row, "url_imagen") else NA_character_
  )

  logo_url <- coalesce_chr(extract_profile_logo_url(page), row_value_or_na(listing_row, "url_logo_partido"))
  ids$partido_id <- coalesce_chr(
    ids$partido_id,
    extract_party_id_from_logo_url(logo_url),
    extract_party_id_from_logo_url(row_value_or_na(listing_row, "url_logo_partido"))
  )
  ids$dni <- coalesce_chr(top_lookup[["dni"]], ids$dni, row_value_or_na(listing_row, "dni_listado"))
  ids$person_id <- if (!is.na(ids$partido_id) && !is.na(ids$dni)) {
    paste(ids$partido_id, ids$dni, sep = "_")
  } else {
    ids$person_id
  }
  ids$id_candidato <- coalesce_chr(
    build_candidate_record_id(
      ids$partido_id,
      ids$dni,
      type = role_metadata$type,
      target_slug = row_value_or_na(district_row, "target_slug")
    ),
    ids$id_candidato
  )

  estudios_tecnicos_block <- if (!is.null(education_section)) {
    find_heading_block(education_section$node, c("estudios_tecnicos"))
  } else if (!is.null(sections[["estudios_tecnicos"]])) {
    sections[["estudios_tecnicos"]]
  } else {
    NULL
  }
  estudios_no_universitarios_block <- if (!is.null(education_section)) {
    find_heading_block(education_section$node, c("estudios_no_universitarios", "no_universitarios"))
  } else if (!is.null(sections[["estudios_no_universitarios"]])) {
    sections[["estudios_no_universitarios"]]
  } else {
    NULL
  }
  estudios_universitarios_block <- if (!is.null(education_section)) {
    find_heading_block(education_section$node, c("estudios_universitarios"))
  } else if (!is.null(sections[["estudios_universitarios"]])) {
    sections[["estudios_universitarios"]]
  } else {
    NULL
  }
  estudios_posgrado_block <- if (!is.null(education_section)) {
    find_heading_block(education_section$node, c("estudios_de_posgrado", "estudios_posgrado"))
  } else if (!is.null(sections[["estudios_posgrado"]])) {
    sections[["estudios_posgrado"]]
  } else {
    NULL
  }
  cargos_partidarios_block <- if (!is.null(trajectory_section)) {
    find_heading_block(trajectory_section$node, c("cargos_partidarios"))
  } else {
    NULL
  }
  eleccion_popular_block <- if (!is.null(trajectory_section)) {
    find_heading_block(trajectory_section$node, c("eleccion_popular", "cargos_de_eleccion_popular"))
  } else {
    NULL
  }

  main_record <- tibble::tibble(
    id_candidato = ids$id_candidato,
    partido_id = ids$partido_id,
    codigo_distrito_electoral = district_row$codigo_distrito_electoral,
    distrito_electoral = district_row$distrito_electoral,
    cargo_postula = role_metadata$cargo_postula,
    type = role_metadata$type,
    dni = ids$dni,
    nombre = coalesce_chr(
      extract_profile_name(page),
      row_value_or_na(listing_row, "nombre")
    ),
    partido_politico = coalesce_chr(top_lookup[["organizacion_politica"]], row_value_or_na(listing_row, "partido_politico")),
    sexo = coalesce_chr(top_lookup[["sexo"]], top_lookup[["genero"]]),
    lugar_nacimiento = top_lookup[["lugar_de_nacimiento"]],
    lugar_domicilio = coalesce_chr(top_lookup[["domicilio"]], top_lookup[["lugar_de_domicilio"]]),
    numero_postulacion = row_value_or_na(listing_row, "numero_postulacion"),
    estado_inadmisible = row_value_or_na(listing_row, "estado_inadmisible"),
    url_hoja_vida = url_hoja_vida,
    url_imagen = image_url,
    ruta_imagen = NA_character_,
    url_logo_partido = logo_url,
    educacion_basica_cuenta = basic_values$cuenta,
    educacion_basica_primaria = basic_values$primaria,
    educacion_basica_secundaria = basic_values$secundaria,
    ingresos_remuneracion_bruta_publico = income_values$remuneracion_bruta_publico,
    ingresos_remuneracion_bruta_privado = income_values$remuneracion_bruta_privado,
    ingresos_total_ingresos = income_values$total_ingresos,
    fecha_captura = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )

  section_data <- list(
    estudios_tecnicos = extract_study_rows_from_block(if (is.null(estudios_tecnicos_block)) NULL else estudios_tecnicos_block$node, ids$id_candidato),
    estudios_no_universitarios = extract_study_rows_from_block(if (is.null(estudios_no_universitarios_block)) NULL else estudios_no_universitarios_block$node, ids$id_candidato),
    estudios_universitarios = extract_study_rows_from_block(if (is.null(estudios_universitarios_block)) NULL else estudios_universitarios_block$node, ids$id_candidato),
    estudios_posgrado = extract_study_rows_from_block(if (is.null(estudios_posgrado_block)) NULL else estudios_posgrado_block$node, ids$id_candidato),
    experiencia_laboral = extract_experience_rows(if (is.null(sections[["experiencia_laboral"]])) NULL else sections[["experiencia_laboral"]]$node, ids$id_candidato),
    cargos_partidarios = extract_trayectoria_rows_from_block(if (is.null(cargos_partidarios_block)) NULL else cargos_partidarios_block$node, ids$id_candidato),
    eleccion_popular = extract_trayectoria_rows_from_block(if (is.null(eleccion_popular_block)) NULL else eleccion_popular_block$node, ids$id_candidato),
    relacion_sentencias = extract_sentence_rows(if (is.null(sentences_section)) NULL else sentences_section$node, ids$id_candidato),
    bienes_muebles_inmuebles = extract_asset_rows(if (is.null(assets_section)) NULL else assets_section$node, ids$id_candidato),
    informacion_adicional = extract_additional_info(if (is.null(additional_section)) NULL else additional_section$node, ids$id_candidato),
    anotacion_marginal = extract_annotation_rows(if (is.null(annotation_section)) NULL else annotation_section$node, ids$id_candidato)
  )

  list(
    main = main_record,
    sections = section_data,
    ids = ids
  )
}


# ==========================================
# ==========  SELENIUM HELPERS =============
# ==========================================

resolve_logical_env <- function(x, default = TRUE) {
  if (is.null(x) || !nzchar(x)) {
    return(default)
  }

  x_clean <- tolower(trimws(x))

  if (x_clean %in% c("1", "true", "t", "yes", "y")) {
    return(TRUE)
  }

  if (x_clean %in% c("0", "false", "f", "no", "n")) {
    return(FALSE)
  }

  default
}

resolve_integer_env <- function(x) {
  if (is.null(x) || !nzchar(x)) {
    return(NA_integer_)
  }

  suppressWarnings(as.integer(x))
}

start_firefox_selenium <- function(headless = TRUE, width = 1600, height = 1200, attempts = 3, log_fn = NULL) {
  # Browser startup can be flaky on some machines, so we retry a few times
  # before treating Selenium startup as a hard failure.
  session <- NULL

  for (selenium_attempt in seq_len(attempts)) {
    firefox_args <- c(
      paste0("--width=", as.integer(width)),
      paste0("--height=", as.integer(height))
    )

    if (isTRUE(headless)) {
      firefox_args <- c(firefox_args, "-headless")
    }

    session <- tryCatch(
      RSelenium::rsDriver(
        port = sample(7600:7699, 1),
        browser = c("firefox"),
        chromever = NULL,
        check = FALSE,
        verbose = FALSE,
        extraCapabilities = list(
          `moz:firefoxOptions` = list(args = firefox_args)
        )
      ),
      error = function(e) {
        if (is.function(log_fn) && selenium_attempt < attempts) {
          log_fn(
            paste(
              "Firefox startup attempt",
              selenium_attempt,
              "failed. Retrying."
            )
          )
        }

        if (selenium_attempt >= attempts) {
          stop(e)
        }

        Sys.sleep(3)
        NULL
      }
    )

    if (!is.null(session)) {
      break
    }
  }

  if (is.null(session)) {
    stop("Unable to start a Firefox RSelenium session.", call. = FALSE)
  }

  session$client$setWindowSize(as.integer(width), as.integer(height))
  session
}

close_firefox_selenium <- function(session) {
  if (is.null(session)) {
    return(invisible(NULL))
  }

  try(session$client$close(), silent = TRUE)
  try(session$server$stop(), silent = TRUE)
  invisible(NULL)
}

selenium_page_source <- function(remDr) {
  tryCatch(remDr$getPageSource()[[1]], error = function(e) NA_character_)
}

selenium_current_url <- function(remDr) {
  tryCatch(remDr$getCurrentUrl()[[1]], error = function(e) NA_character_)
}

profile_html_ready_for_scraper <- function(page_html, current_url = NA_character_) {
  if (is.na(page_html)) {
    return(FALSE)
  }

  html_key <- value_or_na(clean_name_es(coalesce_chr(page_html)))
  loading_skeleton <- !is.na(html_key) && grepl("animate_pulse", html_key, fixed = TRUE)
  empty_name_heading <- grepl("<h1[^>]*>\\s*</h1>", page_html, perl = TRUE)

  if (loading_skeleton || empty_name_heading) {
    return(FALSE)
  }

  profile_markers <- c(
    "informacion_general",
    "formacion_academica",
    "experiencia_laboral",
    "trayectoria_politica",
    "cargos_partidarios",
    "declaracion_de_sentencias_firmes",
    "ingreso_de_bienes_y_rentas",
    "informacion_adicional",
    "anotacion_marginal",
    "lugar_de_nacimiento",
    "domicilio",
    "sentencias_penales",
    "sentencias_no_penales",
    "bienes_muebles_inmuebles"
  )

  marker_match <- any(vapply(profile_markers, function(marker_value) {
    !is.na(html_key) && grepl(marker_value, html_key, fixed = TRUE)
  }, logical(1)))

  on_profile_url <- !is.na(current_url) && grepl("/hoja-vida/[0-9]+/[0-9]+", current_url)
  still_on_results <- !is.na(html_key) && grepl("resultados_de_busqueda", html_key, fixed = TRUE)
  transitioned_profile_page <- on_profile_url &&
    !still_on_results &&
    !is.na(html_key) &&
    nchar(html_key) >= 2000 &&
    !grepl("ver_hoja_de_vida", html_key, fixed = TRUE)

  marker_match || transitioned_profile_page
}

wait_for_profile_page_selenium <- function(remDr, timeout = 15, poll_interval = 0.5) {
  start_time <- Sys.time()

  repeat {
    current_url <- selenium_current_url(remDr)
    page_html <- selenium_page_source(remDr)
    profile_nodes <- tryCatch(
      remDr$findElements(using = "css selector", value = "main section, main h1"),
      error = function(e) list()
    )
    profile_text_match <- profile_html_ready_for_scraper(page_html, current_url = current_url)

    if (length(profile_nodes) > 0 || isTRUE(profile_text_match)) {
      return(TRUE)
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= timeout) {
      return(FALSE)
    }

    Sys.sleep(poll_interval)
  }
}

read_live_profile_photo_url <- function(remDr) {
  result <- tryCatch(
    remDr$executeScript(
      paste(
        "var nodes = Array.from(document.querySelectorAll(\"main img[src*='mpesije.jne.gob.pe'], main img[src*='/apidocs/'], main img.rounded-full.object-cover\"));",
        "for (var i = 0; i < nodes.length; i += 1) {",
        "  var node = nodes[i];",
        "  var value = node.currentSrc || node.getAttribute('src') || node.src || '';",
        "  if (!value) { continue; }",
        "  if (/assets\\/images\\/logojne\\.png(?:\\?.*)?$/i.test(value)) { continue; }",
        "  if (/(^|\\/)foto-perfil\\.png(?:\\?.*)?$/i.test(value)) { continue; }",
        "  return value;",
        "}",
        "return null;"
      )
    ),
    error = function(e) NA_character_
  )

  if (is.list(result) && length(result) > 0) {
    result <- result[[1]]
  }

  photo_url <- make_absolute_url(as.character(result))

  if (is_placeholder_image(photo_url)) {
    return(NA_character_)
  }

  photo_url
}

wait_for_live_profile_photo_url <- function(remDr, timeout = 20, poll_interval = 0.5) {
  start_time <- Sys.time()

  repeat {
    photo_url <- read_live_profile_photo_url(remDr)

    if (!is.na(photo_url)) {
      return(photo_url)
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= timeout) {
      return(NA_character_)
    }

    Sys.sleep(poll_interval)
  }
}

capture_live_profile_html <- function(remDr, required_photo_url = NA_character_, timeout = 5, poll_interval = 0.5) {
  start_time <- Sys.time()
  last_html <- selenium_page_source(remDr)

  repeat {
    page_html <- selenium_page_source(remDr)

    if (!is.na(page_html)) {
      last_html <- page_html
      parsed_photo_url <- tryCatch(
        extract_profile_photo_url(xml2::read_html(page_html)),
        error = function(e) NA_character_
      )

      if (!is.na(parsed_photo_url) && !is_placeholder_image(parsed_photo_url)) {
        return(page_html)
      }

      if (!is.na(required_photo_url) && grepl(required_photo_url, page_html, fixed = TRUE)) {
        return(page_html)
      }
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= timeout) {
      return(last_html)
    }

    Sys.sleep(poll_interval)
  }
}

fetch_live_profile_for_image <- function(remDr,
                                         url_hoja_vida,
                                         profile_load_timeout = 15,
                                         image_load_timeout = 20,
                                         poll_interval = 0.5,
                                         pause_seconds = 0.5) {
  target_url <- value_or_na(url_hoja_vida)

  if (is.na(target_url)) {
    stop("Cannot refresh a live profile without a hoja de vida URL.", call. = FALSE)
  }

  remDr$navigate(target_url)
  Sys.sleep(pause_seconds)

  if (!wait_for_profile_page_selenium(remDr, timeout = profile_load_timeout, poll_interval = poll_interval)) {
    stop("Profile page did not load after navigating to the candidate URL.", call. = FALSE)
  }

  photo_url <- wait_for_live_profile_photo_url(
    remDr,
    timeout = image_load_timeout,
    poll_interval = poll_interval
  )

  if (is.na(photo_url)) {
    stop("Profile portrait did not load after the profile page rendered.", call. = FALSE)
  }

  page_html <- capture_live_profile_html(
    remDr,
    required_photo_url = photo_url,
    timeout = min(5, image_load_timeout),
    poll_interval = poll_interval
  )

  current_url <- selenium_current_url(remDr)

  if (is.na(page_html) || !profile_html_ready_for_scraper(page_html, current_url = current_url)) {
    stop("Rendered profile HTML does not look like a loaded hoja de vida page.", call. = FALSE)
  }

  list(
    url_hoja_vida = current_url,
    url_imagen = photo_url,
    page_html = page_html
  )
}

# ==========================================
# ==========  RAW FILES AND IMAGES =========
# ==========================================

listing_snapshot_prefix <- function(district_row, suffix = NULL) {
  district_row <- tibble::as_tibble(district_row)
  prefix_parts <- c(
    row_value_or_na(district_row, "target_slug"),
    row_value_or_na(district_row, "codigo_distrito_electoral"),
    slugify_path(coalesce_chr(row_value_or_na(district_row, "distrito_electoral"), "sin-distrito"))
  )
  prefix_parts <- prefix_parts[!is.na(prefix_parts) & nzchar(prefix_parts)]
  prefix <- paste(prefix_parts, collapse = "_")

  if (!is.null(suffix) && nzchar(suffix)) {
    prefix <- paste(prefix, slugify_path(suffix), sep = "_")
  }

  prefix
}

save_listing_snapshot <- function(district_row, html, cards, suffix = NULL) {
  prefix <- listing_snapshot_prefix(district_row, suffix = suffix)

  html_path <- file.path("data", "raw", "listados", paste0(prefix, ".html"))
  csv_path <- file.path("data", "raw", "listados", paste0(prefix, "_candidatos.csv"))

  writeLines(html, html_path, useBytes = TRUE)
  write_csv_chr(cards, csv_path)

  invisible(list(html_path = html_path, csv_path = csv_path))
}

save_profile_snapshot <- function(id_candidato, html) {
  html_path <- file.path("data", "raw", "perfiles", paste0(id_candidato, ".html"))
  writeLines(html, html_path, useBytes = TRUE)
  invisible(html_path)
}

build_image_path <- function(main_record) {
  district_slug <- slugify_path(coalesce_chr(main_record$distrito_electoral, "sin-distrito"))
  party_slug <- slugify_path(coalesce_chr(main_record$partido_politico, "sin-partido"))
  name_slug <- slugify_path(coalesce_chr(main_record$nombre, main_record$id_candidato))
  file_name <- paste0(main_record$id_candidato, "_", name_slug, ".jpg")

  file.path("output", "images", district_slug, party_slug, file_name)
}

download_candidate_image <- function(url_imagen, dest_path, overwrite = FALSE) {
  if (is.na(url_imagen) || !nzchar(url_imagen) || is_placeholder_image(url_imagen)) {
    return(NA_character_)
  }

  dir.create(dirname(dest_path), recursive = TRUE, showWarnings = FALSE)

  existing_info <- suppressWarnings(file.info(dest_path))
  existing_size <- as.numeric(existing_info$size[[1]])
  existing_valid <- isTRUE(file.exists(dest_path)) && !is.na(existing_size) && existing_size > 0

  if (existing_valid && !isTRUE(overwrite)) {
    return(normalizePath(dest_path, winslash = "/", mustWork = FALSE))
  }

  if (file.exists(dest_path)) {
    unlink(dest_path, force = TRUE)
  }

  success <- tryCatch({
    utils::download.file(url_imagen, destfile = dest_path, mode = "wb", quiet = TRUE)
    downloaded_info <- suppressWarnings(file.info(dest_path))
    downloaded_size <- as.numeric(downloaded_info$size[[1]])
    isTRUE(file.exists(dest_path)) && !is.na(downloaded_size) && downloaded_size > 0
  }, error = function(e) FALSE, warning = function(w) FALSE)

  if (!isTRUE(success)) {
    if (file.exists(dest_path)) {
      unlink(dest_path, force = TRUE)
    }

    return(NA_character_)
  }

  normalizePath(dest_path, winslash = "/", mustWork = FALSE)
}
