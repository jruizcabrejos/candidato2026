######################################################################################
# Project: Scrapper and processing of Peru's 2026 election candidate data
# Script: 1_scrapper.R - Live collection from the public candidate portal
# Author: Jorge Ruiz-Cabrejos and Inca Slop
# Created: 2026-03-25
# Last Updated: 2026-03-25
######################################################################################

#~#~#~#~#~#~#~#~#~#~#~#~#~#~#
rm(list = ls())
#~#~#~#~#~#~#~#~#~#~#~#~#~#~#


# ================================
# ==========  SETTINGS ===========
# ================================

BASE_URL <- "https://votoinformado.jne.gob.pe/diputados"
EMBED_URL <- "https://votoinformadoia.jne.gob.pe/embed/diputados"

## Keep the browser visible so the run can be watched and debugged.
HEADLESS <- FALSE

## Optional filters for small runs.
DISTRICT_FILTER <- character()
MAX_DISTRICTS <- NA_integer_
MAX_CANDIDATES_PER_DISTRICT <- NA_integer_

## Micro pauses only. Enough to avoid hammering the site, not enough to feel stuck.
PAUSE_AFTER_PAGE_LOAD <- 1
PAUSE_AFTER_DISTRICT_CHANGE <- 1
PAUSE_AFTER_PARTY_CLICK <- 0.5
PAUSE_AFTER_CARD_CLICK <- 0.5
PAUSE_BETWEEN_CANDIDATES_MIN <- 7
PAUSE_BETWEEN_CANDIDATES_MAX <- 7
PAUSE_BETWEEN_DISTRICTS <- 0.5
PAUSE_AFTER_SORT_CHANGE <- 0.25

## Most districts render quickly, but Lima Metropolitana can take longer because
## the embedded listing needs to render around 1,000 candidate cards.
RESULTS_LOAD_TIMEOUT <- 45
PROFILE_LOAD_TIMEOUT <- 15
RESULTS_SORT_VALUE <- "asc"
RESULTS_POLL_INTERVAL <- 1
PROFILE_POLL_INTERVAL <- 0.5

## Optional environment overrides.
## These make it easy to run smaller tests without editing the script itself.
ENV_HEADLESS <- Sys.getenv("JNE_HEADLESS", unset = "")
ENV_DISTRICT_FILTER <- Sys.getenv("JNE_DISTRICT_VALUES", unset = "")
ENV_TARGETS <- Sys.getenv("JNE_TARGETS", unset = "")
ENV_MAX_DISTRICTS <- Sys.getenv("JNE_MAX_DISTRICTS", unset = "")
ENV_MAX_CANDIDATES_PER_DISTRICT <- Sys.getenv("JNE_MAX_CANDIDATES_PER_DISTRICT", unset = "")
ENV_RESULTS_LOAD_TIMEOUT <- Sys.getenv("JNE_RESULTS_LOAD_TIMEOUT", unset = "")
ENV_PROFILE_LOAD_TIMEOUT <- Sys.getenv("JNE_PROFILE_LOAD_TIMEOUT", unset = "")

TARGET_CONFIGS <- list(
  diputados = list(
    id = "diputados",
    label = "Diputados",
    base_url = "https://votoinformado.jne.gob.pe/diputados",
    embed_url = "https://votoinformadoia.jne.gob.pe/embed/diputados"
  ),
  `presidente-vicepresidentes` = list(
    id = "presidente-vicepresidentes",
    label = "Formula presidencial",
    base_url = "https://votoinformado.jne.gob.pe/presidente-vicepresidentes"
  ),
  senadores = list(
    id = "senadores",
    label = "Senadores",
    base_url = "https://votoinformado.jne.gob.pe/senadores"
  )
)

TARGET_FILTER <- names(TARGET_CONFIGS)

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

# Prepare the local folder structure up front so later writes can assume it exists.
if (nzchar(ENV_HEADLESS)) {
  HEADLESS <- resolve_logical_env(ENV_HEADLESS, default = HEADLESS)
}

if (nzchar(ENV_DISTRICT_FILTER)) {
  DISTRICT_FILTER <- ENV_DISTRICT_FILTER %>%
    strsplit(",", fixed = TRUE) %>%
    unlist(use.names = FALSE) %>%
    trimws() %>%
    .[nzchar(.)]
}

if (nzchar(ENV_TARGETS)) {
  TARGET_FILTER <- ENV_TARGETS %>%
    strsplit(",", fixed = TRUE) %>%
    unlist(use.names = FALSE) %>%
    trimws() %>%
    .[nzchar(.)]

  unknown_targets <- setdiff(TARGET_FILTER, names(TARGET_CONFIGS))

  if (length(unknown_targets) > 0) {
    stop(
      paste(
        "Unknown JNE_TARGETS values:",
        paste(unknown_targets, collapse = ", ")
      ),
      call. = FALSE
    )
  }
}

ENV_MAX_DISTRICTS <- resolve_integer_env(ENV_MAX_DISTRICTS)
if (!is.na(ENV_MAX_DISTRICTS)) {
  MAX_DISTRICTS <- ENV_MAX_DISTRICTS
}

ENV_MAX_CANDIDATES_PER_DISTRICT <- resolve_integer_env(ENV_MAX_CANDIDATES_PER_DISTRICT)
if (!is.na(ENV_MAX_CANDIDATES_PER_DISTRICT)) {
  MAX_CANDIDATES_PER_DISTRICT <- ENV_MAX_CANDIDATES_PER_DISTRICT
}

ENV_RESULTS_LOAD_TIMEOUT <- resolve_integer_env(ENV_RESULTS_LOAD_TIMEOUT)
if (!is.na(ENV_RESULTS_LOAD_TIMEOUT)) {
  RESULTS_LOAD_TIMEOUT <- ENV_RESULTS_LOAD_TIMEOUT
}

ENV_PROFILE_LOAD_TIMEOUT <- resolve_integer_env(ENV_PROFILE_LOAD_TIMEOUT)
if (!is.na(ENV_PROFILE_LOAD_TIMEOUT)) {
  PROFILE_LOAD_TIMEOUT <- ENV_PROFILE_LOAD_TIMEOUT
}


# ======================================
# ==========  LOCAL HELPERS ============
# ======================================

log_message <- function(text) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), text))
}

current_page_source <- function() {
  tryCatch(remDr$getPageSource()[[1]], error = function(e) NA_character_)
}

current_url_value <- function() {
  tryCatch(remDr$getCurrentUrl()[[1]], error = function(e) NA_character_)
}

pause_between_candidates_local <- function(card_idx, total_candidates) {
  if (card_idx >= total_candidates) {
    return(invisible(FALSE))
  }

  pause_min <- suppressWarnings(as.numeric(PAUSE_BETWEEN_CANDIDATES_MIN))
  pause_max <- suppressWarnings(as.numeric(PAUSE_BETWEEN_CANDIDATES_MAX))

  if (is.na(pause_min) || is.na(pause_max)) {
    return(invisible(FALSE))
  }

  pause_floor <- max(0, min(pause_min, pause_max))
  pause_ceiling <- max(0, max(pause_min, pause_max))

  if (pause_ceiling <= 0) {
    return(invisible(FALSE))
  }

  pause_seconds <- if (isTRUE(all.equal(pause_floor, pause_ceiling))) {
    pause_ceiling
  } else {
    stats::runif(1, min = pause_floor, max = pause_ceiling)
  }

  log_message(sprintf("    Waiting %.1fs before the next candidate.", pause_seconds))
  Sys.sleep(pause_seconds)

  invisible(TRUE)
}

peru_scope_row_local <- function(target_slug, cargo_postula = NA_character_, type = NA_character_) {
  tibble::tibble(
    codigo_distrito_electoral = "PERU",
    distrito_electoral = "Peru",
    cargo_postula = cargo_postula,
    type = type,
    target_slug = target_slug
  )
}

wait_for_url_match_local <- function(pattern, timeout = 20) {
  start_time <- Sys.time()

  repeat {
    current_url <- current_url_value()

    if (!is.na(current_url) && grepl(pattern, current_url, perl = TRUE)) {
      return(current_url)
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= timeout) {
      return(NA_character_)
    }

    Sys.sleep(1)
  }
}

wait_for_parsed_rows_local <- function(parse_fn, timeout = 20, poll_interval = 1) {
  start_time <- Sys.time()
  last_html <- NA_character_
  last_rows <- tibble::tibble()

  repeat {
    page_html <- current_page_source()

    if (!is.na(page_html)) {
      last_html <- page_html
      page <- tryCatch(xml2::read_html(page_html), error = function(e) NULL)

      if (!is.null(page)) {
        parsed_rows <- tryCatch(parse_fn(page), error = function(e) tibble::tibble())
        last_rows <- parsed_rows

        if (!is.null(parsed_rows) && nrow(parsed_rows) > 0) {
          return(list(html = page_html, rows = parsed_rows))
        }
      }
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= timeout) {
      return(list(html = last_html, rows = last_rows))
    }

    Sys.sleep(poll_interval)
  }
}

find_scope_card_nodes_local <- function(selector_value) {
  tryCatch(
    remDr$findElements(using = "css selector", value = selector_value),
    error = function(e) list()
  )
}

click_scope_card_by_index_local <- function(selector_value, card_index, pause_seconds = PAUSE_AFTER_CARD_CLICK) {
  scope_nodes <- find_scope_card_nodes_local(selector_value)

  if (length(scope_nodes) < card_index) {
    stop("Scope card index does not exist on the current page.", call. = FALSE)
  }

  target_node <- scope_nodes[[card_index]]
  scroll_node_local(target_node)
  Sys.sleep(1)
  target_node$clickElement()
  Sys.sleep(pause_seconds)

  invisible(TRUE)
}

extract_senate_party_scope_rows_local <- function(page, target_slug = "senadores") {
  cards <- rvest::html_elements(
    page,
    "div.relative.border.border-gray-200.bg-white.p-6.rounded-lg.cursor-pointer"
  )

  if (length(cards) == 0) {
    return(tibble::tibble(
      card_index = integer(),
      partido_politico = character()
    ))
  }

  purrr::imap_dfr(cards, function(card, idx) {
    tibble::tibble(
      card_index = as.integer(idx),
      partido_politico = safe_html_text(card),
      target_slug = target_slug
    )
  }) %>%
    dplyr::filter(!is.na(.data$partido_politico))
}

extract_district_options_local <- function(page) {
  option_nodes <- rvest::html_elements(page, "#departamento option")

  if (length(option_nodes) == 0) {
    return(tibble::tibble(
      codigo_distrito_electoral = character(),
      distrito_electoral = character()
    ))
  }

  tibble::tibble(
    codigo_distrito_electoral = purrr::map_chr(option_nodes, ~ value_or_na(rvest::html_attr(.x, "value"))),
    distrito_electoral = purrr::map_chr(option_nodes, ~ value_or_na(rvest::html_text2(.x)))
  ) %>%
    dplyr::filter(!is.na(.data$codigo_distrito_electoral), !is.na(.data$distrito_electoral))
}

open_listing_page_local <- function() {
  remDr$navigate(BASE_URL)
  Sys.sleep(PAUSE_AFTER_PAGE_LOAD)

  start_time <- Sys.time()

  repeat {
    dropdown_nodes <- tryCatch(
      remDr$findElements(using = "css selector", value = "#departamento"),
      error = function(e) list()
    )

    if (length(dropdown_nodes) > 0) {
      return(invisible(TRUE))
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= 45) {
      stop("The diputados district selector did not load.", call. = FALSE)
    }

    Sys.sleep(1)
  }
}

open_embed_page_local <- function() {
  remDr$navigate(EMBED_URL)
  Sys.sleep(PAUSE_AFTER_PAGE_LOAD)

  start_time <- Sys.time()

  repeat {
    html <- current_page_source()

    if (!is.na(html) &&
        (grepl("Departamento electoral", html, fixed = TRUE) ||
         grepl("Selecciona tu departamento", html, fixed = TRUE) ||
         grepl("rs__control", html, fixed = TRUE))) {
      return(invisible(TRUE))
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= 45) {
      stop("The embedded diputados department selector did not load.", call. = FALSE)
    }

    Sys.sleep(1)
  }
}

normalize_text_local <- function(x) {
  value_or_na(clean_name_es(coalesce_chr(x)))
}

embed_district_lookup_local <- c(
  amazonas = "Amazonas",
  ancash = "Áncash",
  apurimac = "Apurímac",
  arequipa = "Arequipa",
  ayacucho = "Ayacucho",
  cajamarca = "Cajamarca",
  callao = "Callao",
  cusco = "Cusco",
  huancavelica = "Huancavelica",
  huanuco = "Huánuco",
  ica = "Ica",
  junin = "Junín",
  la_libertad = "La Libertad",
  lambayeque = "Lambayeque",
  lima = "Lima",
  loreto = "Loreto",
  madre_de_dios = "Madre de Dios",
  moquegua = "Moquegua",
  pasco = "Pasco",
  piura = "Piura",
  puno = "Puno",
  san_martin = "San Martín",
  tacna = "Tacna",
  tumbes = "Tumbes",
  ucayali = "Ucayali"
)

canonicalize_embed_district_local <- function(district_label) {
  district_key <- clean_name_es(district_label)

  if (!is.na(district_key) && district_key %in% names(embed_district_lookup_local)) {
    return(embed_district_lookup_local[[district_key]])
  }

  district_label %>%
    tolower() %>%
    stringr::str_replace_all("_", " ") %>%
    tools::toTitleCase()
}

scroll_node_local <- function(node) {
  try(remDr$executeScript("arguments[0].scrollIntoView({block: 'center'});", list(node)), silent = TRUE)
  invisible(node)
}

extract_node_text_local <- function(node) {
  tryCatch(
    value_or_na(node$getElementText()[[1]]),
    error = function(e) NA_character_
  )
}

extract_node_attribute_local <- function(node, attribute_name) {
  tryCatch(
    value_or_na(node$getElementAttribute(attribute_name)[[1]]),
    error = function(e) NA_character_
  )
}

find_result_card_nodes_local <- function() {
  selector_candidates <- c(
    "div.block.bg-white.border.border-gray-200.rounded-lg.shadow-sm",
    "div[class*='block'][class*='bg-white'][class*='border-gray-200'][class*='rounded-lg'][class*='shadow-sm']"
  )

  for (selector_value in selector_candidates) {
    card_nodes <- tryCatch(
      remDr$findElements(using = "css selector", value = selector_value),
      error = function(e) list()
    )

    if (length(card_nodes) == 0) {
      next
    }

    card_nodes <- Filter(function(card_node) {
      card_text <- extract_node_text_local(card_node)
      !is.na(card_text) && grepl("Ver hoja de vida", card_text, fixed = TRUE)
    }, card_nodes)

    if (length(card_nodes) > 0) {
      return(card_nodes)
    }
  }

  list()
}

extract_listing_cards_from_dom_local <- function(district_row) {
  card_nodes <- find_result_card_nodes_local()
  profile_base_url <- coalesce_chr(current_url_value(), EMBED_URL)

  if (length(card_nodes) == 0) {
    return(tibble::tibble(
      card_index = integer(),
      codigo_distrito_electoral = character(),
      distrito_electoral = character(),
      numero_postulacion = character(),
      nombre = character(),
      dni_listado = character(),
      partido_politico = character(),
      estado_inadmisible = character(),
      url_imagen = character(),
      url_logo_partido = character(),
      url_hoja_vida = character(),
      card_html = character()
    ))
  }

  purrr::imap_dfr(card_nodes, function(card_node, idx) {
    card_text <- extract_node_text_local(card_node)
    card_lines <- compact_lines(split_multiline_text(card_text))

    card_lines <- card_lines[!grepl("^Ver hoja de vida$", card_lines, ignore.case = TRUE)]
    card_lines <- card_lines[!grepl("^Guardar perfil$", card_lines, ignore.case = TRUE)]
    dni_listado <- value_or_na(stringr::str_match(card_text, "DNI\\s*:?\\s*([0-9]{7,10})")[, 2])

    title_node <- tryCatch(
      {
        title_nodes <- card_node$findChildElements(using = "xpath", value = ".//h3 | .//h2")
        if (length(title_nodes) > 0) title_nodes[[1]] else NULL
      },
      error = function(e) NULL
    )

    candidate_name <- coalesce_chr(
      if (!is.null(title_node)) extract_node_text_local(title_node) else NA_character_,
      value_or_na(card_lines[1])
    )

    party_node <- tryCatch(
      {
        party_nodes <- card_node$findChildElements(
          using = "xpath",
          value = ".//span[contains(@class, 'text-sm') and contains(@class, 'font-medium')]"
        )
        if (length(party_nodes) > 0) party_nodes[[1]] else NULL
      },
      error = function(e) NULL
    )

    party_candidates <- card_lines[
      !grepl("^DNI\\s*:", card_lines, ignore.case = TRUE) &
        !grepl("^SIN ", card_lines, ignore.case = TRUE) &
        !grepl("^[0-9]+$", card_lines)
    ]

    party_candidates <- party_candidates[party_candidates != candidate_name]
    party_name <- coalesce_chr(
      if (!is.null(party_node)) extract_node_text_local(party_node) else NA_character_,
      value_or_na(party_candidates[1])
    )

    image_node <- tryCatch(
      {
        image_nodes <- card_node$findChildElements(using = "css selector", value = "img")
        if (length(image_nodes) > 0) image_nodes[[1]] else NULL
      },
      error = function(e) NULL
    )

    party_logo_node <- tryCatch(
      {
        party_logo_nodes <- card_node$findChildElements(
          using = "xpath",
          value = ".//div[contains(@class, 'flex') and contains(@class, 'items-center') and contains(@class, 'space-x-2')]//img"
        )
        if (length(party_logo_nodes) > 0) party_logo_nodes[[1]] else NULL
      },
      error = function(e) NULL
    )

    profile_link_node <- tryCatch(
      {
        profile_link_nodes <- card_node$findChildElements(
          using = "xpath",
          value = ".//a[contains(@href, '/hoja-vida/')]"
        )
        if (length(profile_link_nodes) > 0) profile_link_nodes[[1]] else NULL
      },
      error = function(e) NULL
    )

    image_url <- if (!is.null(image_node)) {
      make_absolute_url(
        extract_node_attribute_local(image_node, "src"),
        base = "https://votoinformadoia.jne.gob.pe"
      )
    } else {
      NA_character_
    }

    party_logo_url <- if (!is.null(party_logo_node)) {
      make_absolute_url(
        extract_node_attribute_local(party_logo_node, "src"),
        base = "https://votoinformadoia.jne.gob.pe"
      )
    } else {
      NA_character_
    }

    profile_url <- coalesce_chr(
      if (!is.null(profile_link_node)) {
        resolve_profile_url_local(extract_node_attribute_local(profile_link_node, "href"))
      } else {
        NA_character_
      },
      build_profile_url_from_listing(
        party_logo_url,
        dni_listado,
        base = profile_base_url
      )
    )

    tibble::tibble(
      card_index = as.integer(idx),
      codigo_distrito_electoral = district_row$codigo_distrito_electoral,
      distrito_electoral = district_row$distrito_electoral,
      numero_postulacion = NA_character_,
      nombre = candidate_name,
      dni_listado = dni_listado,
      partido_politico = party_name,
      estado_inadmisible = NA_character_,
      url_imagen = image_url,
      url_logo_partido = party_logo_url,
      url_hoja_vida = profile_url,
      card_html = NA_character_
    )
  })
}

read_results_status_text_local <- function() {
  result <- tryCatch(
    remDr$executeScript(
      paste(
        "var text = document.body ? document.body.innerText : '';",
        "var match = text.match(/Se encontraron\\s+\\d+\\s+candidatos\\s+de\\s+\\d+\\s+candidatos\\s+totales/i);",
        "return match ? match[0] : null;"
      )
    ),
    error = function(e) NA_character_
  )

  if (is.list(result) && length(result) > 0) {
    result <- result[[1]]
  }

  value_or_na(as.character(result))
}

read_loading_status_text_local <- function() {
  result <- tryCatch(
    remDr$executeScript(
      paste(
        "var text = document.body ? document.body.innerText : '';",
        "var match = text.match(/Buscando candidatos/i);",
        "return match ? match[0] : null;"
      )
    ),
    error = function(e) NA_character_
  )

  if (is.list(result) && length(result) > 0) {
    result <- result[[1]]
  }

  value_or_na(as.character(result))
}

cards_are_sorted_by_name_local <- function(cards, direction = RESULTS_SORT_VALUE) {
  if (is.null(cards) || nrow(cards) <= 1 || !"nombre" %in% names(cards)) {
    return(TRUE)
  }

  card_names <- vapply(cards$nombre, function(name_value) {
    normalize_text_local(name_value)
  }, character(1))

  card_names[is.na(card_names)] <- "zzzzzzzz"
  expected_names <- sort(card_names, decreasing = identical(direction, "desc"))
  identical(card_names, expected_names)
}

read_results_sort_value_local <- function() {
  result <- tryCatch(
    remDr$executeScript(
      paste(
        "var node = document.querySelector('#sort-order');",
        "return node ? node.value : null;"
      )
    ),
    error = function(e) NA_character_
  )

  if (is.list(result) && length(result) > 0) {
    result <- result[[1]]
  }

  value_or_na(as.character(result))
}

profile_html_ready_local <- function(page_html, current_url = NA_character_) {
  if (is.na(page_html)) {
    return(FALSE)
  }

  html_key <- normalize_text_local(page_html)
  loading_skeleton <- !is.na(html_key) && grepl("animate_pulse", html_key, fixed = TRUE)
  empty_name_heading <- grepl("<h1[^>]*>\\s*</h1>", page_html, perl = TRUE)
  loading_copy <- !is.na(html_key) && grepl("cargando_hoja_de_vida", html_key, fixed = TRUE)

  if (loading_skeleton || empty_name_heading || loading_copy) {
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
    "bienes_muebles_inmuebles",
    "content_postulacion_comparacion",
    "educacion_basica",
    "estudios_tecnicos",
    "estudios_no_universitarios",
    "estudios_universitarios",
    "estudios_de_posgrado",
    "relacion_de_sentencias",
    "ingresos_de_bienes_y_rentas"
  )
  detail_markers <- c(
    "dni",
    "sexo",
    "lugar_de_nacimiento",
    "lugar_de_domicilio",
    "cargo_al_que_postula"
  )

  marker_match <- any(vapply(profile_markers, function(marker_value) {
    !is.na(html_key) && grepl(marker_value, html_key, fixed = TRUE)
  }, logical(1)))
  detail_match <- any(vapply(detail_markers, function(marker_value) {
    !is.na(html_key) && grepl(marker_value, html_key, fixed = TRUE)
  }, logical(1)))

  on_profile_url <- !is.na(current_url) && grepl("/hoja-vida/[0-9]+/[0-9]+", current_url)
  still_on_results <- !is.na(html_key) && grepl("resultados_de_busqueda", html_key, fixed = TRUE)
  transitioned_profile_page <- on_profile_url && !still_on_results && marker_match && detail_match

  marker_match && detail_match || transitioned_profile_page
}

sort_results_listing_local <- function(district_row, target_value = RESULTS_SORT_VALUE, timeout = RESULTS_LOAD_TIMEOUT) {
  sort_nodes <- tryCatch(
    remDr$findElements(using = "css selector", value = "#sort-order"),
    error = function(e) list()
  )

  if (length(sort_nodes) == 0) {
    return(wait_for_candidate_snapshot_local(district_row, timeout = timeout))
  }

  current_sort_value <- read_results_sort_value_local()

  if (!identical(current_sort_value, target_value)) {
    sort_applied <- tryCatch(
      remDr$executeScript(
        paste(
          "var node = document.querySelector('#sort-order');",
          "if (!node) { return false; }",
          "node.value = arguments[0];",
          "node.dispatchEvent(new Event('input', { bubbles: true }));",
          "node.dispatchEvent(new Event('change', { bubbles: true }));",
          "return node.value === arguments[0];"
        ),
        list(target_value)
      ),
      error = function(e) FALSE
    )

    if (!isTRUE(sort_applied)) {
      return(wait_for_candidate_snapshot_local(district_row, timeout = timeout))
    }

    Sys.sleep(PAUSE_AFTER_SORT_CHANGE)
  }

  start_time <- Sys.time()
  last_snapshot <- wait_for_candidate_snapshot_local(district_row, timeout = timeout)

  repeat {
    current_sort_value <- read_results_sort_value_local()
    current_cards <- extract_listing_cards_from_dom_local(district_row)

    if (nrow(current_cards) > 0) {
      last_snapshot <- list(
        html = current_page_source(),
        cards = current_cards,
        status_ready = TRUE,
        status_text = read_results_status_text_local()
      )
    } else {
      last_snapshot <- wait_for_candidate_snapshot_local(district_row, timeout = RESULTS_POLL_INTERVAL)
    }

    if (identical(current_sort_value, target_value) &&
        nrow(last_snapshot$cards) > 0 &&
        cards_are_sorted_by_name_local(last_snapshot$cards, direction = target_value)) {
      return(last_snapshot)
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= timeout) {
      return(last_snapshot)
    }

    Sys.sleep(RESULTS_POLL_INTERVAL)
  }
}

current_results_snapshot_local <- function(district_row, timeout = 3) {
  page_html <- current_page_source()
  page_key <- normalize_text_local(page_html)

  if (is.na(page_key) || !grepl("resultados_de_busqueda", page_key, fixed = TRUE)) {
    return(NULL)
  }

  snapshot <- wait_for_candidate_snapshot_local(district_row, timeout = timeout)

  if (nrow(snapshot$cards) == 0) {
    return(NULL)
  }

  current_sort_value <- read_results_sort_value_local()

  if (!identical(current_sort_value, RESULTS_SORT_VALUE) ||
      !cards_are_sorted_by_name_local(snapshot$cards, direction = RESULTS_SORT_VALUE)) {
    snapshot <- sort_results_listing_local(
      district_row,
      target_value = RESULTS_SORT_VALUE,
      timeout = RESULTS_LOAD_TIMEOUT
    )
  }

  snapshot
}

find_button_by_patterns_local <- function(include_patterns, exclude_patterns = character()) {
  button_nodes <- tryCatch(
    remDr$findElements(using = "xpath", value = "//button[not(@disabled)]"),
    error = function(e) list()
  )

  if (length(button_nodes) == 0) {
    return(NULL)
  }

  for (button_node in button_nodes) {
    button_text <- normalize_text_local(extract_node_text_local(button_node))

    if (is.na(button_text)) {
      next
    }

    include_match <- any(vapply(include_patterns, function(pattern) {
      grepl(pattern, button_text, perl = TRUE)
    }, logical(1)))

    exclude_match <- any(vapply(exclude_patterns, function(pattern) {
      grepl(pattern, button_text, perl = TRUE)
    }, logical(1)))

    if (include_match && !exclude_match) {
      return(button_node)
    }
  }

  NULL
}

click_button_local <- function(button_node, pause_seconds = 1) {
  if (is.null(button_node)) {
    stop("Target button was not found in the rendered DOM.", call. = FALSE)
  }

  scroll_node_local(button_node)
  Sys.sleep(1)
  button_node$clickElement()
  Sys.sleep(pause_seconds)

  invisible(TRUE)
}

read_embed_selected_district_local <- function() {
  result <- tryCatch(
    remDr$executeScript(
      paste(
        "var node = document.querySelector('.rs__single-value');",
        "return node ? node.textContent.trim() : null;"
      )
    ),
    error = function(e) NA_character_
  )

  if (is.list(result) && length(result) > 0) {
    result <- result[[1]]
  }

  value_or_na(as.character(result))
}

set_embed_district_local <- function(district_row) {
  target_label <- canonicalize_embed_district_local(district_row$distrito_electoral[[1]])
  target_key <- normalize_text_local(target_label)

  for (attempt in seq_len(3)) {
    control_node <- tryCatch(
      remDr$findElement(using = "css selector", value = ".rs__control"),
      error = function(e) NULL
    )

    if (is.null(control_node)) {
      Sys.sleep(1)
      next
    }

    scroll_node_local(control_node)
    try(control_node$clickElement(), silent = TRUE)
    Sys.sleep(1)

    input_nodes <- tryCatch(
      remDr$findElements(using = "css selector", value = ".rs__input input"),
      error = function(e) list()
    )

    input_node <- if (length(input_nodes) > 0) input_nodes[[1]] else NULL

    if (is.null(input_node)) {
      input_nodes <- tryCatch(
        remDr$findElements(using = "css selector", value = "input[id^='react-select-']"),
        error = function(e) list()
      )

      input_node <- if (length(input_nodes) > 0) input_nodes[[1]] else NULL
    }

    if (is.null(input_node)) {
      try(remDr$sendKeysToActiveElement(strsplit(target_label, "")[[1]]), silent = TRUE)
    } else {
      try(input_node$sendKeysToElement(list(key = "home")), silent = TRUE)
      try(input_node$sendKeysToElement(strsplit(target_label, "")[[1]]), silent = TRUE)
    }

    Sys.sleep(1)

    option_nodes <- tryCatch(
      remDr$findElements(using = "css selector", value = ".rs__option"),
      error = function(e) list()
    )

    matched_option <- NULL

    for (option_node in option_nodes) {
      option_text <- normalize_text_local(extract_node_text_local(option_node))

      if (!is.na(option_text) && identical(option_text, target_key)) {
        matched_option <- option_node
        break
      }
    }

    if (!is.null(matched_option)) {
      click_button_local(matched_option, pause_seconds = PAUSE_AFTER_DISTRICT_CHANGE)
    } else {
      if (is.null(input_node)) {
        try(remDr$sendKeysToActiveElement(list(key = "enter")), silent = TRUE)
      } else {
        try(input_node$sendKeysToElement(list(key = "enter")), silent = TRUE)
      }

      Sys.sleep(PAUSE_AFTER_DISTRICT_CHANGE)
    }

    selected_text <- normalize_text_local(read_embed_selected_district_local())

    if (!is.na(selected_text) && identical(selected_text, target_key)) {
      return(TRUE)
    }
  }

  FALSE
}

wait_for_embed_search_view_local <- function(timeout = 45) {
  start_time <- Sys.time()

  repeat {
    html <- current_page_source()
    html_key <- normalize_text_local(html)

    if (!is.na(html_key) &&
        grepl("candidatos_a_diputados_por", html_key) &&
        grepl("busqueda_con_filtros", html_key)) {
      return(TRUE)
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= timeout) {
      return(FALSE)
    }

    Sys.sleep(1)
  }
}

activate_filters_tab_local <- function() {
  filters_button <- find_button_by_patterns_local(
    include_patterns = c("^busqueda_con_filtros$"),
    exclude_patterns = c("buscar_candidatos")
  )

  if (is.null(filters_button)) {
    return(FALSE)
  }

  click_button_local(filters_button, pause_seconds = 1)
  TRUE
}

submit_filters_local <- function() {
  submit_button <- find_button_by_patterns_local(
    include_patterns = c(
      "^ver_resultados$",
      "buscar_candidatos",
      "^buscar$",
      "aplicar_filtros",
      "^filtrar$",
      "^ver_candidatos$"
    ),
    exclude_patterns = c(
      "busqueda_con_filtros",
      "busqueda_con_eleccia",
      "inteligencia_artificial",
      "cambiar_region"
    )
  )

  if (is.null(submit_button)) {
    stop("The filtros submit button was not found after selecting the district.", call. = FALSE)
  }

  click_button_local(submit_button, pause_seconds = PAUSE_AFTER_PARTY_CLICK)
  invisible(TRUE)
}

wait_for_candidate_snapshot_local <- function(district_row, timeout = RESULTS_LOAD_TIMEOUT) {
  start_time <- Sys.time()
  last_html <- NA_character_
  last_cards <- tibble::tibble()
  last_status_ready <- FALSE
  last_status_text <- NA_character_

  repeat {
    status_text <- read_results_status_text_local()
    loading_text <- read_loading_status_text_local()
    dom_cards <- extract_listing_cards_from_dom_local(district_row)
    dom_loading <- !is.na(loading_text)

    if (!is.na(status_text) || nrow(dom_cards) > 0) {
      last_status_ready <- TRUE
      last_status_text <- status_text
    }

    if (nrow(dom_cards) > 0 && (last_status_ready || !dom_loading)) {
      html <- current_page_source()
      last_html <- html
      last_cards <- dom_cards

      return(list(
        html = html,
        cards = dom_cards,
        status_ready = last_status_ready,
        status_text = last_status_text
      ))
    }

    html <- current_page_source()
    last_html <- html

    if (!is.na(html)) {
      page <- tryCatch(xml2::read_html(html), error = function(e) NULL)

      if (!is.null(page)) {
        cards <- extract_listing_cards(page, district_row)

        if (nrow(cards) == 0 && nrow(dom_cards) > 0) {
          cards <- dom_cards
        }

        last_cards <- cards
        html_key <- normalize_text_local(html)
        loading <- !is.na(loading_text) ||
          grepl("Cargando", html, ignore.case = TRUE) ||
          (!is.na(html_key) && grepl("buscando_candidatos", html_key, fixed = TRUE))
        results_ready <- !is.na(status_text) ||
          nrow(dom_cards) > 0 ||
          (!is.na(html_key) && grepl("se_encontraron_.*candidatos_totales", html_key, perl = TRUE))

        if (results_ready) {
          last_status_ready <- TRUE
          last_status_text <- coalesce_chr(
            status_text,
            stringr::str_match(
              html_key,
              "(se_encontraron_.*candidatos_totales)"
            )[, 2]
          )
        }

        if (nrow(cards) > 0 && (last_status_ready || !loading)) {
          return(list(
            html = html,
            cards = cards,
            status_ready = results_ready,
            status_text = last_status_text
          ))
        }
      }
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= timeout) {
      return(list(
        html = last_html,
        cards = last_cards,
        status_ready = last_status_ready,
        status_text = last_status_text
      ))
    }

    Sys.sleep(RESULTS_POLL_INTERVAL)
  }
}

open_results_listing_for_district_local <- function(district_row) {
  # The candidate cards are rendered through the embedded app, so we drive that
  # flow first and only continue once the results page is actually populated.
  open_embed_page_local()

  if (!set_embed_district_local(district_row)) {
    stop("Unable to select the district in the embedded diputados selector.", call. = FALSE)
  }

  district_button <- find_button_by_patterns_local(
    include_patterns = c("^ver_candidatos_en"),
    exclude_patterns = c("busqueda_con_filtros", "busqueda_con_eleccia")
  )

  if (is.null(district_button)) {
    stop("The embedded district submit button did not appear after selecting the district.", call. = FALSE)
  }

  click_button_local(district_button, pause_seconds = PAUSE_AFTER_DISTRICT_CHANGE)

  if (!wait_for_embed_search_view_local(timeout = 20)) {
    stop("The filtros view did not appear after selecting the district.", call. = FALSE)
  }

  activate_filters_tab_local()
  submit_filters_local()

  snapshot <- wait_for_candidate_snapshot_local(district_row, timeout = RESULTS_LOAD_TIMEOUT)

  if (nrow(snapshot$cards) == 0) {
    if (isTRUE(snapshot$status_ready)) {
      stop(
        paste(
          "The page reported candidate totals, but the scraper could not parse the candidate cards.",
          coalesce_chr(snapshot$status_text)
        ),
        call. = FALSE
      )
    }

    stop(
      paste(
        "No candidate cards were rendered after the embedded district flow within",
        paste0(RESULTS_LOAD_TIMEOUT, "s.")
      ),
      call. = FALSE
    )
  }

  sorted_snapshot <- sort_results_listing_local(
    district_row,
    target_value = RESULTS_SORT_VALUE,
    timeout = RESULTS_LOAD_TIMEOUT
  )

  if (nrow(sorted_snapshot$cards) > 0) {
    return(sorted_snapshot)
  }

  snapshot
}

resolve_profile_url_local <- function(url_value) {
  url_value <- coalesce_chr(url_value)

  if (is.na(url_value)) {
    return(NA_character_)
  }

  current_url <- current_url_value()
  current_base <- if (!is.na(current_url) && grepl("votoinformadoia\\.jne\\.gob\\.pe", current_url)) {
    "https://votoinformadoia.jne.gob.pe"
  } else {
    "https://votoinformado.jne.gob.pe"
  }

  relative_match <- stringr::str_match(url_value, "(\\/hoja-vida\\/[0-9]+\\/[0-9]+)")

  if (!all(is.na(relative_match[1, ]))) {
    return(make_absolute_url(relative_match[1, 2], base = current_base))
  }

  url_value
}

find_clickable_candidate_card_nodes_local <- function() {
  selector_candidates <- c(
    ".div-container-candidato",
    "div.bg-white.border.border-gray-200.rounded-lg.p-4.shadow-sm.w-64.text-center.cursor-pointer",
    "div.block.bg-white.border.border-gray-200.rounded-lg.shadow-sm",
    "div[class*='block'][class*='bg-white'][class*='border-gray-200'][class*='rounded-lg'][class*='shadow-sm']"
  )

  for (selector_value in selector_candidates) {
    card_nodes <- tryCatch(
      remDr$findElements(using = "css selector", value = selector_value),
      error = function(e) list()
    )

    if (length(card_nodes) == 0) {
      next
    }

    if (grepl("^div\\.block", selector_value)) {
      card_nodes <- Filter(function(card_node) {
        card_text <- extract_node_text_local(card_node)
        !is.na(card_text) && grepl("Ver hoja de vida", card_text, fixed = TRUE)
      }, card_nodes)
    }

    if (length(card_nodes) > 0) {
      return(card_nodes)
    }
  }

  list()
}

click_candidate_card_node_local <- function(target_card) {
  scroll_node_local(target_card)
  Sys.sleep(PAUSE_AFTER_CARD_CLICK)

  profile_buttons <- tryCatch(
    target_card$findChildElements(
      using = "xpath",
      value = ".//button[contains(normalize-space(.), 'Ver hoja de vida')] | .//a[contains(normalize-space(.), 'Ver hoja de vida')]"
    ),
    error = function(e) list()
  )

  if (length(profile_buttons) > 0) {
    profile_buttons[[1]]$clickElement()
  } else {
    target_card$clickElement()
  }

  Sys.sleep(PAUSE_AFTER_CARD_CLICK)
  invisible(TRUE)
}

open_candidate_profile_local <- function(district_row, card_row) {
  hinted_url <- coalesce_chr(
    resolve_profile_url_local(coalesce_chr(card_row$url_hoja_vida)),
    build_profile_url_from_listing(
      coalesce_chr(card_row$url_logo_partido),
      coalesce_chr(card_row$dni_listado),
      base = current_url_value()
    )
  )

  if (!is.na(hinted_url)) {
    remDr$navigate(hinted_url)
    Sys.sleep(PAUSE_AFTER_CARD_CLICK)

    if (!wait_for_profile_page_local(timeout = PROFILE_LOAD_TIMEOUT)) {
      stop("Profile page did not load after navigating to the candidate URL.", call. = FALSE)
    }

    return(invisible(hinted_url))
  }

  matched_card_index <- suppressWarnings(as.integer(card_row$card_index[[1]]))
  card_nodes <- find_clickable_candidate_card_nodes_local()

  if (is.na(matched_card_index) || length(card_nodes) < matched_card_index) {
    stop("Candidate card index does not exist on the current page.", call. = FALSE)
  }

  click_candidate_card_node_local(card_nodes[[matched_card_index]])

  if (!wait_for_profile_page_local(timeout = PROFILE_LOAD_TIMEOUT)) {
    stop("Profile page did not load after clicking the candidate card.", call. = FALSE)
  }

  invisible(TRUE)
}

wait_for_profile_page_local <- function(timeout = PROFILE_LOAD_TIMEOUT) {
  start_time <- Sys.time()

  repeat {
    current_url <- tryCatch(remDr$getCurrentUrl()[[1]], error = function(e) NA_character_)
    page_html <- current_page_source()
    profile_text_match <- profile_html_ready_local(page_html, current_url = current_url)

    if (isTRUE(profile_text_match)) {
      return(TRUE)
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= timeout) {
      return(FALSE)
    }

    Sys.sleep(PROFILE_POLL_INTERVAL)
  }
}

merge_saved_scope_cards_local <- function(cards, scope_row, snapshot_suffix) {
  cards <- tibble::as_tibble(cards)

  if (nrow(cards) == 0) {
    return(cards)
  }

  snapshot_csv_path <- file.path(
    "data",
    "raw",
    "listados",
    paste0(listing_snapshot_prefix(scope_row, suffix = snapshot_suffix), "_candidatos.csv")
  )

  saved_cards <- read_csv_chr(snapshot_csv_path)

  if (is.null(saved_cards) || nrow(saved_cards) == 0 || !"card_index" %in% names(saved_cards)) {
    return(cards)
  }

  merge_cols <- intersect(
    names(cards),
    c(
      "dni_listado",
      "partido_politico",
      "cargo_postula",
      "type",
      "url_imagen",
      "url_logo_partido",
      "url_hoja_vida"
    )
  )

  if (length(merge_cols) == 0) {
    return(cards)
  }

  cards <- cards %>%
    dplyr::mutate(card_index = as.character(.data$card_index))
  saved_cards <- saved_cards %>%
    dplyr::mutate(card_index = as.character(.data$card_index))

  saved_lookup <- saved_cards %>%
    dplyr::select("card_index", dplyr::all_of(merge_cols)) %>%
    dplyr::distinct(.data$card_index, .keep_all = TRUE)

  merged_cards <- cards %>%
    dplyr::left_join(saved_lookup, by = "card_index", suffix = c("", "_saved"))

  for (col_name in merge_cols) {
    saved_col <- paste0(col_name, "_saved")
    merged_cards[[col_name]] <- purrr::pmap_chr(
      list(merged_cards[[col_name]], merged_cards[[saved_col]]),
      function(current_value, saved_value) {
        coalesce_chr(current_value, saved_value)
      }
    )
  }

  merged_cards %>%
    dplyr::select(dplyr::all_of(names(cards)))
}

update_scope_cards_with_profile_local <- function(cards, card_row, parsed_candidate, current_url, current_ids) {
  cards <- tibble::as_tibble(cards)

  if (nrow(cards) == 0 || !"card_index" %in% names(cards)) {
    return(cards)
  }

  target_index <- suppressWarnings(as.integer(card_row$card_index[[1]]))

  if (is.na(target_index)) {
    return(cards)
  }

  match_idx <- which(suppressWarnings(as.integer(cards$card_index)) == target_index)

  if (length(match_idx) == 0) {
    return(cards)
  }

  row_idx <- match_idx[[1]]
  cards$nombre[[row_idx]] <- coalesce_chr(parsed_candidate$main$nombre[[1]], cards$nombre[[row_idx]])
  cards$dni_listado[[row_idx]] <- coalesce_chr(current_ids$dni, parsed_candidate$main$dni[[1]], cards$dni_listado[[row_idx]])
  cards$partido_politico[[row_idx]] <- coalesce_chr(parsed_candidate$main$partido_politico[[1]], cards$partido_politico[[row_idx]])
  cards$cargo_postula[[row_idx]] <- coalesce_chr(parsed_candidate$main$cargo_postula[[1]], cards$cargo_postula[[row_idx]])
  cards$type[[row_idx]] <- coalesce_chr(parsed_candidate$main$type[[1]], cards$type[[row_idx]])
  cards$url_imagen[[row_idx]] <- coalesce_chr(parsed_candidate$main$url_imagen[[1]], cards$url_imagen[[row_idx]])
  cards$url_logo_partido[[row_idx]] <- coalesce_chr(parsed_candidate$main$url_logo_partido[[1]], cards$url_logo_partido[[row_idx]])
  cards$url_hoja_vida[[row_idx]] <- coalesce_chr(current_url, cards$url_hoja_vida[[row_idx]])

  cards
}

process_scope_candidates_local <- function(scope_label,
                                           scope_row,
                                           scope_url,
                                           scope_html,
                                           cards,
                                           snapshot_suffix,
                                           restore_scope_fn = NULL,
                                           completed_ids) {
  working_cards <- merge_saved_scope_cards_local(cards, scope_row, snapshot_suffix)

  save_listing_snapshot(
    scope_row,
    scope_html,
    working_cards,
    suffix = snapshot_suffix
  )

  cards_to_process <- working_cards

  if (!is.na(MAX_CANDIDATES_PER_DISTRICT)) {
    cards_to_process <- cards_to_process %>% dplyr::slice_head(n = MAX_CANDIDATES_PER_DISTRICT)
  }

  log_message(paste("  Candidate cards detected:", nrow(cards_to_process)))

  if (nrow(cards_to_process) == 0) {
    return(list(cards = working_cards, completed_ids = completed_ids))
  }

  candidatos_path <- file.path("output", "candidatos.csv")

  for (card_idx in seq_len(nrow(cards_to_process))) {
    card_row <- cards_to_process[card_idx, , drop = FALSE]
    skip_candidate <- FALSE
    did_open_profile <- FALSE
    hinted_url <- NA_character_

    log_message(
      paste0(
        "  Candidate ", card_idx, "/", nrow(cards_to_process), ": ",
        coalesce_chr(card_row$nombre, paste("card", card_idx))
      )
    )

    tryCatch({
      if (is.function(restore_scope_fn) && is.na(coalesce_chr(card_row$url_hoja_vida))) {
        restore_scope_fn()
      }

      hinted_url <- coalesce_chr(
        resolve_profile_url_local(coalesce_chr(card_row$url_hoja_vida)),
        build_profile_url_from_listing(
          coalesce_chr(card_row$url_logo_partido),
          coalesce_chr(card_row$dni_listado),
          base = current_url_value()
        )
      )
      hinted_ids <- derive_candidate_ids(
        hinted_url,
        type = row_value_or_na(card_row, "type"),
        target_slug = row_value_or_na(scope_row, "target_slug")
      )

      if (!is.na(hinted_ids$id_candidato) && hinted_ids$id_candidato %in% completed_ids) {
        log_message(paste("    Skipping completed candidate:", hinted_ids$id_candidato))
        skip_candidate <- TRUE
      }

      if (!skip_candidate) {
        open_candidate_profile_local(scope_row, card_row)
        did_open_profile <- TRUE
      }

      if (!skip_candidate) {
        current_url <- resolve_profile_url_local(current_url_value())
        current_ids <- derive_candidate_ids(
          current_url,
          type = row_value_or_na(card_row, "type"),
          target_slug = row_value_or_na(scope_row, "target_slug")
        )

        if (is.na(current_ids$id_candidato)) {
          stop("Unable to derive id_candidato from the current profile URL.")
        }

        if (current_ids$id_candidato %in% completed_ids) {
          log_message(paste("    Already persisted:", current_ids$id_candidato))
          skip_candidate <- TRUE
        }
      }

      if (!skip_candidate) {
        profile_html <- current_page_source()

        if (is.na(profile_html) || !profile_html_ready_local(profile_html, current_url = current_url)) {
          stop("Rendered profile HTML does not look like a loaded hoja de vida page.")
        }

        save_profile_snapshot(current_ids$id_candidato, profile_html)

        profile_page <- xml2::read_html(profile_html)
        parsed_candidate <- parse_candidate_profile(profile_page, card_row, scope_row, current_url)

        image_dest <- build_image_path(parsed_candidate$main[1, ])
        image_path <- download_candidate_image(parsed_candidate$main$url_imagen[[1]], image_dest)
        parsed_candidate$main$ruta_imagen <- image_path

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
        completed_ids <- unique(c(completed_ids, parsed_candidate$ids$id_candidato))
        working_cards <- update_scope_cards_with_profile_local(
          working_cards,
          card_row,
          parsed_candidate,
          current_url,
          current_ids
        )

        save_listing_snapshot(
          scope_row,
          scope_html,
          working_cards,
          suffix = snapshot_suffix
        )

        log_message(paste("    Saved:", parsed_candidate$ids$id_candidato))
      }
    }, error = function(e) {
      append_error_row(tibble::tibble(
        fecha = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        distrito_electoral = scope_row$distrito_electoral,
        codigo_distrito_electoral = scope_row$codigo_distrito_electoral,
        card_index = as.character(card_row$card_index[[1]]),
        url_hoja_vida = hinted_url,
        id_candidato = coalesce_chr(
          derive_candidate_ids(
            hinted_url,
            type = row_value_or_na(card_row, "type"),
            target_slug = row_value_or_na(scope_row, "target_slug")
          )$id_candidato
        ),
        error = paste(scope_label, ":", e$message)
      ))

      log_message(paste("    Error:", e$message))
      Sys.sleep(1)
    })

    if (isTRUE(did_open_profile)) {
      pause_between_candidates_local(card_idx, nrow(cards_to_process))
    }
  }

  list(cards = working_cards, completed_ids = completed_ids)
}

# ======================================
# ==========  START RSELENIUM ===========
# ======================================

browser_mode_label <- if (isTRUE(HEADLESS)) "headless" else "visible"

log_message(paste("Starting", browser_mode_label, "Peru 2026 election candidate scrapper."))
log_message(paste("Starting a single", browser_mode_label, "Firefox RSelenium session."))
rD <- start_firefox_selenium(
  headless = HEADLESS,
  log_fn = log_message
)

remDr <- rD$client

on.exit(close_firefox_selenium(rD), add = TRUE)


# ======================================
# ==========  SCRAPING LOOP =============
# ======================================

selected_targets <- TARGET_CONFIGS[TARGET_FILTER]
completed_ids <- completed_candidate_ids()

log_message(paste("Targets queued:", paste(names(selected_targets), collapse = ", ")))
log_message(paste("Candidates already marked complete:", length(completed_ids)))

for (target_id in names(selected_targets)) {
  target <- selected_targets[[target_id]]
  log_message(paste("Starting target:", target$id))

  if (identical(target$id, "diputados")) {
    BASE_URL <- target$base_url
    EMBED_URL <- target$embed_url

    log_message("Opening diputados listing page.")
    open_listing_page_local()

    listing_html <- current_page_source()

    if (is.na(listing_html)) {
      stop("Unable to read the rendered diputados page.", call. = FALSE)
    }

    listing_page <- xml2::read_html(listing_html)
    districts <- extract_district_options_local(listing_page) %>%
      dplyr::mutate(
        cargo_postula = "DIPUTADO",
        type = "Diputado",
        target_slug = target$id
      )

    if (nrow(districts) == 0) {
      stop("No district options were found in the diputados selector.", call. = FALSE)
    }

    if (length(DISTRICT_FILTER) > 0) {
      districts <- districts %>%
        dplyr::filter(
          .data$codigo_distrito_electoral %in% DISTRICT_FILTER |
            .data$distrito_electoral %in% DISTRICT_FILTER
        )
    }

    if (!is.na(MAX_DISTRICTS)) {
      districts <- districts %>% dplyr::slice_head(n = MAX_DISTRICTS)
    }

    log_message(paste("Districts queued:", nrow(districts)))

    for (district_idx in seq_len(nrow(districts))) {
      district_row <- districts[district_idx, , drop = FALSE]

      log_message(
        paste0(
          "District ", district_idx, "/", nrow(districts), ": ",
          district_row$distrito_electoral, " [", district_row$codigo_distrito_electoral, "]"
        )
      )

      tryCatch({
        candidate_snapshot <- open_results_listing_for_district_local(district_row)
        cards <- tibble::as_tibble(candidate_snapshot$cards)

        if (!"cargo_postula" %in% names(cards)) {
          cards$cargo_postula <- NA_character_
        }

        if (!"type" %in% names(cards)) {
          cards$type <- NA_character_
        }

        cards <- cards %>%
          dplyr::mutate(
            cargo_postula = purrr::map_chr(.data$cargo_postula, ~ coalesce_chr(.x, "DIPUTADO")),
            type = purrr::map_chr(.data$type, ~ coalesce_chr(.x, "Diputado"))
          )

        scope_result <- process_scope_candidates_local(
          scope_label = "Diputados",
          scope_row = district_row,
          scope_url = current_url_value(),
          scope_html = candidate_snapshot$html,
          cards = cards,
          snapshot_suffix = "resultados",
          restore_scope_fn = NULL,
          completed_ids = completed_ids
        )

        completed_ids <- scope_result$completed_ids
      }, error = function(e) {
        append_error_row(tibble::tibble(
          fecha = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
          distrito_electoral = district_row$distrito_electoral,
          codigo_distrito_electoral = district_row$codigo_distrito_electoral,
          card_index = NA_character_,
          url_hoja_vida = NA_character_,
          id_candidato = NA_character_,
          error = paste("Diputados:", e$message)
        ))

        log_message(paste("  Error:", e$message))
        Sys.sleep(1)
      })

      checkpoint_info <- tryCatch(
        save_district_checkpoint(district_row, district_idx = district_idx),
        error = function(e) e
      )

      if (inherits(checkpoint_info, "error")) {
        log_message(paste("  Checkpoint error:", checkpoint_info$message))
      } else {
        log_message(
          paste0(
            "  Checkpoint saved: ",
            checkpoint_info$dir,
            " (",
            length(checkpoint_info$files),
            " files)"
          )
        )
      }

      log_message(paste("Finished district:", district_row$distrito_electoral))
      Sys.sleep(PAUSE_BETWEEN_DISTRICTS)
    }

    next
  }

  if (identical(target$id, "presidente-vicepresidentes")) {
    scope_row <- peru_scope_row_local(target$id)
    remDr$navigate(target$base_url)
    Sys.sleep(PAUSE_AFTER_PAGE_LOAD)

    formula_scope_snapshot <- wait_for_parsed_rows_local(
      function(page) extract_listing_cards(page, scope_row),
      timeout = RESULTS_LOAD_TIMEOUT
    )

    formula_scope_rows <- formula_scope_snapshot$rows

    if (nrow(formula_scope_rows) == 0) {
      stop("No presidential formula cards were found on the landing page.", call. = FALSE)
    }

    if (!is.na(MAX_DISTRICTS)) {
      formula_scope_rows <- formula_scope_rows %>% dplyr::slice_head(n = MAX_DISTRICTS)
    }

    log_message(paste("Formulas queued:", nrow(formula_scope_rows)))

    for (formula_idx in seq_len(nrow(formula_scope_rows))) {
      formula_row <- formula_scope_rows[formula_idx, , drop = FALSE]

      log_message(
        paste0(
          "Formula ", formula_idx, "/", nrow(formula_scope_rows), ": ",
          coalesce_chr(formula_row$partido_politico, paste("formula", formula_idx))
        )
      )

      tryCatch({
        remDr$navigate(target$base_url)
        Sys.sleep(PAUSE_AFTER_PAGE_LOAD)

        refreshed_scope <- wait_for_parsed_rows_local(
          function(page) extract_listing_cards(page, scope_row),
          timeout = RESULTS_LOAD_TIMEOUT
        )

        if (nrow(refreshed_scope$rows) == 0) {
          stop("Presidential landing cards did not reload before clicking the formula card.", call. = FALSE)
        }

        click_scope_card_by_index_local(".div-container-candidato", as.integer(formula_row$card_index[[1]]))

        formula_url <- wait_for_url_match_local("/formula-presidencial/[0-9]+$", timeout = 20)

        if (is.na(formula_url)) {
          stop("The formula-presidencial page did not open after clicking the party card.", call. = FALSE)
        }

        formula_snapshot <- wait_for_parsed_rows_local(
          function(page) extract_formula_candidate_cards(page, scope_row),
          timeout = RESULTS_LOAD_TIMEOUT
        )

        formula_cards <- formula_snapshot$rows %>%
          dplyr::mutate(
            partido_politico = dplyr::coalesce(.data$partido_politico, formula_row$partido_politico)
          )

        if (nrow(formula_cards) == 0) {
          stop("No formula members were found on the formula-presidencial page.", call. = FALSE)
        }

        snapshot_suffix <- paste("formula", slugify_path(coalesce_chr(formula_row$partido_politico, formula_url)), sep = "_")

        restore_scope_fn <- function() {
          remDr$navigate(formula_url)
          Sys.sleep(PAUSE_AFTER_PAGE_LOAD)

          restored_scope <- wait_for_parsed_rows_local(
            function(page) extract_formula_candidate_cards(page, scope_row),
            timeout = RESULTS_LOAD_TIMEOUT
          )

          if (nrow(restored_scope$rows) == 0) {
            stop("Formula member cards did not reload after returning to the formula page.", call. = FALSE)
          }

          invisible(restored_scope)
        }

        scope_result <- process_scope_candidates_local(
          scope_label = "Formula presidencial",
          scope_row = scope_row,
          scope_url = formula_url,
          scope_html = formula_snapshot$html,
          cards = formula_cards,
          snapshot_suffix = snapshot_suffix,
          restore_scope_fn = restore_scope_fn,
          completed_ids = completed_ids
        )

        completed_ids <- scope_result$completed_ids
      }, error = function(e) {
        append_error_row(tibble::tibble(
          fecha = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
          distrito_electoral = scope_row$distrito_electoral,
          codigo_distrito_electoral = scope_row$codigo_distrito_electoral,
          card_index = as.character(formula_row$card_index[[1]]),
          url_hoja_vida = current_url_value(),
          id_candidato = NA_character_,
          error = paste("Formula presidencial:", e$message)
        ))

        log_message(paste("  Error:", e$message))
        Sys.sleep(1)
      })

      checkpoint_info <- tryCatch(
        save_district_checkpoint(scope_row, district_idx = formula_idx),
        error = function(e) e
      )

      if (inherits(checkpoint_info, "error")) {
        log_message(paste("  Checkpoint error:", checkpoint_info$message))
      } else {
        log_message(
          paste0(
            "  Checkpoint saved: ",
            checkpoint_info$dir,
            " (",
            length(checkpoint_info$files),
            " files)"
          )
        )
      }

      log_message(paste("Finished formula:", coalesce_chr(formula_row$partido_politico, paste("formula", formula_idx))))
      Sys.sleep(PAUSE_BETWEEN_DISTRICTS)
    }

    next
  }

  if (identical(target$id, "senadores")) {
    scope_row <- peru_scope_row_local(
      target$id,
      cargo_postula = "SENADOR",
      type = "Senador"
    )
    remDr$navigate(target$base_url)
    Sys.sleep(PAUSE_AFTER_PAGE_LOAD)

    senate_scope_snapshot <- wait_for_parsed_rows_local(
      function(page) extract_senate_party_scope_rows_local(page, target_slug = target$id),
      timeout = RESULTS_LOAD_TIMEOUT
    )

    senate_scope_rows <- senate_scope_snapshot$rows

    if (nrow(senate_scope_rows) == 0) {
      stop("No senate party cards were found on the senadores landing page.", call. = FALSE)
    }

    if (!is.na(MAX_DISTRICTS)) {
      senate_scope_rows <- senate_scope_rows %>% dplyr::slice_head(n = MAX_DISTRICTS)
    }

    log_message(paste("Senate parties queued:", nrow(senate_scope_rows)))

    for (party_idx in seq_len(nrow(senate_scope_rows))) {
      party_row <- senate_scope_rows[party_idx, , drop = FALSE]

      log_message(
        paste0(
          "Party ", party_idx, "/", nrow(senate_scope_rows), ": ",
          coalesce_chr(party_row$partido_politico, paste("party", party_idx))
        )
      )

      tryCatch({
        remDr$navigate(target$base_url)
        Sys.sleep(PAUSE_AFTER_PAGE_LOAD)

        refreshed_scope <- wait_for_parsed_rows_local(
          function(page) extract_senate_party_scope_rows_local(page, target_slug = target$id),
          timeout = RESULTS_LOAD_TIMEOUT
        )

        if (nrow(refreshed_scope$rows) == 0) {
          stop("Senate party cards did not reload before clicking the party card.", call. = FALSE)
        }

        click_scope_card_by_index_local(
          "div.relative.border.border-gray-200.bg-white.p-6.rounded-lg.cursor-pointer",
          as.integer(party_row$card_index[[1]])
        )

        party_url <- wait_for_url_match_local("senadores\\?partido=", timeout = 20)

        if (is.na(party_url)) {
          stop("The senate party page did not open after clicking the party card.", call. = FALSE)
        }

        senate_candidate_snapshot <- wait_for_parsed_rows_local(
          function(page) extract_listing_cards(page, scope_row),
          timeout = RESULTS_LOAD_TIMEOUT
        )

        senate_cards <- senate_candidate_snapshot$rows %>%
          dplyr::mutate(
            partido_politico = dplyr::coalesce(.data$partido_politico, party_row$partido_politico),
            cargo_postula = dplyr::coalesce(.data$cargo_postula, "SENADOR"),
            type = dplyr::coalesce(.data$type, "Senador")
          )

        if (nrow(senate_cards) == 0) {
          stop("No senate candidate cards were found on the party page.", call. = FALSE)
        }

        snapshot_suffix <- paste("partido", slugify_path(coalesce_chr(party_row$partido_politico, party_url)), sep = "_")

        restore_scope_fn <- function() {
          remDr$navigate(party_url)
          Sys.sleep(PAUSE_AFTER_PAGE_LOAD)

          restored_scope <- wait_for_parsed_rows_local(
            function(page) extract_listing_cards(page, scope_row),
            timeout = RESULTS_LOAD_TIMEOUT
          )

          if (nrow(restored_scope$rows) == 0) {
            stop("Senate candidate cards did not reload after returning to the party page.", call. = FALSE)
          }

          invisible(restored_scope)
        }

        scope_result <- process_scope_candidates_local(
          scope_label = "Senadores",
          scope_row = scope_row,
          scope_url = party_url,
          scope_html = senate_candidate_snapshot$html,
          cards = senate_cards,
          snapshot_suffix = snapshot_suffix,
          restore_scope_fn = restore_scope_fn,
          completed_ids = completed_ids
        )

        completed_ids <- scope_result$completed_ids
      }, error = function(e) {
        append_error_row(tibble::tibble(
          fecha = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
          distrito_electoral = scope_row$distrito_electoral,
          codigo_distrito_electoral = scope_row$codigo_distrito_electoral,
          card_index = as.character(party_row$card_index[[1]]),
          url_hoja_vida = current_url_value(),
          id_candidato = NA_character_,
          error = paste("Senadores:", e$message)
        ))

        log_message(paste("  Error:", e$message))
        Sys.sleep(1)
      })

      checkpoint_info <- tryCatch(
        save_district_checkpoint(scope_row, district_idx = party_idx),
        error = function(e) e
      )

      if (inherits(checkpoint_info, "error")) {
        log_message(paste("  Checkpoint error:", checkpoint_info$message))
      } else {
        log_message(
          paste0(
            "  Checkpoint saved: ",
            checkpoint_info$dir,
            " (",
            length(checkpoint_info$files),
            " files)"
          )
        )
      }

      log_message(paste("Finished party:", coalesce_chr(party_row$partido_politico, paste("party", party_idx))))
      Sys.sleep(PAUSE_BETWEEN_DISTRICTS)
    }
  }
}

log_message("Run complete.")
