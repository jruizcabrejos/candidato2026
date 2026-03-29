# Recopilacion y Procesamiento de Datos: Elecciones Peru 2026

Este repositorio contiene un flujo local para recopilar, reconstruir, procesar y exportar datos publicos de candidaturas para las elecciones de Peru 2026.

## Como Funciona

1. `scripts/1_scrapper.R`
   Recolecta datos en vivo desde el portal publico del JNE usando la configuracion existente de RSelenium con Firefox visible. Actualmente cubre `diputados`, `presidente-vicepresidentes` y `senadores`.
2. `scripts/2_backfill_saved_profiles.R`
   Reconstruye los CSV normalizados a partir del HTML guardado en `data/raw/`.
3. `scripts/3_average_candidate_faces.R`
   Procesa retratos, genera manifiestos de calidad y produce rostros promedio.
        - Cada JPG se normaliza: Se detecta el rostro principal con `opencv::ocv_facemask()`, se recorta alrededor de la cara (o se usa un recorte de respaldo centrado en el retrato, si no hay deteccion), y se reescala a un lienzo comun de `427x602`.
          Luego se calculan metricas simples de calidad (`blur`, tamano relativo del rostro, centrado, contraste y exposicion). El promedio final se renderiza con `magick::image_average()` sobre esos retratos ya alineados. Es un promedio 2D por pixeles, no una "fusión" (morph) por landmarks faciales (ojos/nariz/boca/...).
4. `scripts/4_average_face_qc_report.Rmd`
   Renderiza un reporte HTML de revision para los rostros promedio.
5. `scripts/5_export_album_bundle.R`
   Arma el paquete final de exportacion con JSON, manifiestos e imagenes.
6. `scripts/6_average_face_transparent.R`
   Quita el fondo blanco de los rostros promedio y guarda versiones `*_transparent.png`.

## Fuentes Principales

- [JNE Voto Informado - Diputados](https://votoinformado.jne.gob.pe/diputados)
- [JNE Voto Informado - Presidente y Vicepresidentes](https://votoinformado.jne.gob.pe/presidente-vicepresidentes)
- [JNE Voto Informado - Senadores](https://votoinformado.jne.gob.pe/senadores)
- [JNE DNE - Voto Informado](https://dne.jne.gob.pe/informacion-electoral/voto-informado)

## Notas Clave

- El scraper en vivo debe correrse visible, no headless, usando la configuracion Firefox ya incluida en el proyecto.
- Las candidaturas exportadas conservan el campo `type`, por ejemplo `Diputado`, `Senador`, `Presidente` y `Vicepresidente`.
- Las candidaturas de `presidente-vicepresidentes` y `senadores` usan `Peru` como ambito geografico.

Este proyecto es un pipeline no oficial construido sobre informacion electoral publica. No esta afiliado, respaldado ni mantenido por el Jurado Nacional de Elecciones.
