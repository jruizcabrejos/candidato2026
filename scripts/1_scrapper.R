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
PAUSE_BETWEEN_CANDIDATES_MIN <- 0
PAUSE_BETWEEN_CANDIDATES_MAX <- 0
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
ENV_MAX_DISTRICTS <- Sys.getenv("JNE_MAX_DISTRICTS", unset = "")
ENV_MAX_CANDIDATES_PER_DISTRICT <- Sys.getenv("JNE_MAX_CANDIDATES_PER_DISTRICT", unset = "")
ENV_RESULTS_LOAD_TIMEOUT <- Sys.getenv("JNE_RESULTS_LOAD_TIMEOUT", unset = "")
ENV_PROFILE_LOAD_TIMEOUT <- Sys.getenv("JNE_PROFILE_LOAD_TIMEOUT", unset = "")

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

  listing_snapshot <- current_results_snapshot_local(district_row, timeout = 2)

  if (is.null(listing_snapshot) || nrow(listing_snapshot$cards) == 0) {
    listing_snapshot <- open_results_listing_for_district_local(district_row)
  }
  matched_card_index <- NA_integer_

  if (nrow(listing_snapshot$cards) > 0) {
    listing_name_keys <- vapply(listing_snapshot$cards$nombre, normalize_text_local, character(1))
    listing_party_keys <- vapply(listing_snapshot$cards$partido_politico, normalize_text_local, character(1))
    target_name_key <- normalize_text_local(card_row$nombre[[1]])
    target_party_key <- normalize_text_local(card_row$partido_politico[[1]])

    matching_rows <- which(
      listing_name_keys == target_name_key &
        (
          is.na(coalesce_chr(card_row$dni_listado)) |
            listing_snapshot$cards$dni_listado == coalesce_chr(card_row$dni_listado)
        )
    )

    if (length(matching_rows) == 0) {
      matching_rows <- which(
        listing_name_keys == target_name_key &
          listing_party_keys == target_party_key
      )
    }

    if (length(matching_rows) > 0) {
      matched_card_index <- matching_rows[[1]]
    }
  }

  if (is.na(matched_card_index)) {
    matched_card_index <- suppressWarnings(as.integer(card_row$card_index[[1]]))
  }

  if (is.na(matched_card_index) || nrow(listing_snapshot$cards) < matched_card_index) {
    stop("Candidate card index does not exist in the rendered resultados listing.", call. = FALSE)
  }

  card_nodes <- tryCatch(
    remDr$findElements(using = "css selector", value = ".div-container-candidato"),
    error = function(e) list()
  )

  if (length(card_nodes) < matched_card_index) {
    card_nodes <- tryCatch(
      remDr$findElements(
        using = "css selector",
        value = "div.block.bg-white.border.border-gray-200.rounded-lg.shadow-sm"
      ),
      error = function(e) list()
    )

    if (length(card_nodes) > 0) {
      card_nodes <- Filter(function(node) {
        node_text <- extract_node_text_local(node)
        nzchar(node_text) && grepl("Ver hoja de vida", node_text, fixed = TRUE)
      }, card_nodes)
    }
  }

  if (length(card_nodes) < matched_card_index) {
    card_nodes <- tryCatch(
      remDr$findElements(using = "xpath", value = "//a[contains(@href, '/hoja-vida/')]"),
      error = function(e) list()
    )
  }

  if (length(card_nodes) < matched_card_index) {
    stop("Candidate card nodes are not available in the rendered resultados listing.", call. = FALSE)
  }

  target_card <- card_nodes[[matched_card_index]]
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
    profile_nodes <- tryCatch(
      remDr$findElements(using = "css selector", value = "main section, main h1"),
      error = function(e) list()
    )
    profile_text_match <- profile_html_ready_local(page_html, current_url = current_url)

    if (length(profile_nodes) > 0 || isTRUE(profile_text_match)) {
      return(TRUE)
    }

    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= timeout) {
      return(FALSE)
    }

    Sys.sleep(PROFILE_POLL_INTERVAL)
  }
}

# ======================================
# ==========  START RSELENIUM ===========
# ======================================

browser_mode_label <- if (isTRUE(HEADLESS)) "headless" else "visible"

log_message(paste("Starting", browser_mode_label, "Peru 2026 election candidate scrapper."))
log_message(paste("Starting a single", browser_mode_label, "Firefox RSelenium session."))

# Firefox / geckodriver startup can fail on a cold start, so we retry a few times
# before giving up on the whole run.
rD <- NULL

for (selenium_attempt in seq_len(3)) {
  firefox_args <- c("--width=1600", "--height=1200")

  if (isTRUE(HEADLESS)) {
    firefox_args <- c(firefox_args, "-headless")
  }

  rD <- tryCatch(
    rsDriver(
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
      if (selenium_attempt < 3) {
        log_message(
          paste(
            "Firefox startup attempt",
            selenium_attempt,
            "failed. Retrying."
          )
        )
        Sys.sleep(3)
        return(NULL)
      }

      stop(e)
    }
  )

  if (!is.null(rD)) {
    break
  }
}

remDr <- rD$client
remDr$setWindowSize(1600, 1200)

on.exit({
  try(remDr$close(), silent = TRUE)
  try(rD$server$stop(), silent = TRUE)
}, add = TRUE)


# ======================================
# ==========  LOAD DISTRICTS ============
# ======================================

log_message("Opening diputados listing page.")
open_listing_page_local()

listing_html <- current_page_source()

if (is.na(listing_html)) {
  stop("Unable to read the rendered diputados page.", call. = FALSE)
}

listing_page <- xml2::read_html(listing_html)
districts <- extract_district_options_local(listing_page)

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


# ======================================
# ==========  SCRAPING LOOP =============
# ======================================

candidatos_path <- file.path("output", "candidatos.csv")
completed_ids <- completed_candidate_ids()

# Keep the run resumable by persisting each candidate independently.
log_message(paste("Candidates already marked complete:", length(completed_ids)))

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

    save_listing_snapshot(
      district_row,
      candidate_snapshot$html,
      candidate_snapshot$cards,
      suffix = "resultados"
    )

    cards <- candidate_snapshot$cards

    if (!is.na(MAX_CANDIDATES_PER_DISTRICT)) {
      cards <- cards %>% dplyr::slice_head(n = MAX_CANDIDATES_PER_DISTRICT)
    }

    log_message(paste("  Candidate cards detected:", nrow(cards)))

    for (card_idx in seq_len(nrow(cards))) {
      # Process one candidate at a time so partial reruns can skip work that has
      # already been safely persisted in previous attempts.
      card_row <- cards[card_idx, , drop = FALSE]
      skip_candidate <- FALSE

      log_message(
        paste0(
          "  Candidate ", card_idx, "/", nrow(cards), ": ",
          coalesce_chr(card_row$nombre, paste("card", card_idx))
        )
      )

      tryCatch({
        hinted_url <- coalesce_chr(
          resolve_profile_url_local(coalesce_chr(card_row$url_hoja_vida)),
          build_profile_url_from_listing(
            coalesce_chr(card_row$url_logo_partido),
            coalesce_chr(card_row$dni_listado),
            base = current_url_value()
          )
        )
        hinted_ids <- derive_candidate_ids(hinted_url)

        if (!is.na(hinted_ids$id_candidato) && hinted_ids$id_candidato %in% completed_ids) {
          log_message(paste("    Skipping completed candidate:", hinted_ids$id_candidato))
          skip_candidate <- TRUE
        }

        if (!skip_candidate) {
          open_candidate_profile_local(district_row, card_row)
        }

        if (!skip_candidate) {
          current_url <- resolve_profile_url_local(current_url_value())
          current_ids <- derive_candidate_ids(current_url)

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
          parsed_candidate <- parse_candidate_profile(profile_page, card_row, district_row, current_url)

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

          log_message(paste("    Saved:", parsed_candidate$ids$id_candidato))
        }
      }, error = function(e) {
        append_error_row(tibble::tibble(
          fecha = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
          distrito_electoral = district_row$distrito_electoral,
          codigo_distrito_electoral = district_row$codigo_distrito_electoral,
          card_index = as.character(card_row$card_index[[1]]),
          url_hoja_vida = hinted_url,
          id_candidato = coalesce_chr(derive_candidate_ids(hinted_url)$id_candidato),
          error = e$message
        ))

        log_message(paste("    Error:", e$message))
        Sys.sleep(1)
      })
    }
  }, error = function(e) {
    append_error_row(tibble::tibble(
      fecha = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      distrito_electoral = district_row$distrito_electoral,
      codigo_distrito_electoral = district_row$codigo_distrito_electoral,
      card_index = NA_character_,
      url_hoja_vida = NA_character_,
      id_candidato = NA_character_,
      error = e$message
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

log_message("Run complete.")
