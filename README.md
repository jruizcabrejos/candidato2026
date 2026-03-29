# 🗳️ Recopilación y Procesamiento de Datos: Elecciones Perú 2026

Este repositorio contiene un flujo local para recopilar, reconstruir, procesar y exportar datos publicos de candidaturas para las elecciones de Peru 2026.

## 📊 Estado Actual (al 26 de marzo de 2026)

*   **27** listados de distritos recopilados.
*   **4,356** perfiles individuales de candidatos guardados.
*   **4,356** fotografías de postulantes descargadas.
*   Datos de candidatos pertenecientes a **37** partidos políticos.

## ⚙️ Cómo Funciona el Proceso

*   **Paso 1: Recopilación (`scripts/1_scrapper.R`)**
    Navega el sitio público web, guarda la información cruda (HTML), descarga las fotografías y actualiza los archivos de datos (CSV).
*   **Paso 2: Reconstrucción (`scripts/2_backfill_saved_profiles.R`)**
    Lee la información guardada localmente y reconstruye los datos normalizados sin necesidad de internet. Solo recurre al navegador web si necesita recuperar alguna foto faltante.
*   **Paso 3: Procesamiento de rostros (`scripts/3_average_candidate_faces.R`)**
    Evalúa la calidad de las fotos, asigna etiquetas de sexo basándose en los valores declarados y genera imágenes compuestas de "rostros promedio".
    
      - Cada JPG se normaliza:
          - Se detecta el rostro principal con opencv::ocv_facemask()
          - Se recorta alrededor de la cara (o se usa un recorte de respaldo centrado en el retrato, si no hay deteccion)
          - Se reescala a un lienzo comun de 427x602
          - Se calculan metricas de "calidad" (blur, tamano relativo del rostro, centrado, contraste y exposicion).
      - El promedio final se renderiza con magick::image_average()
          - Es un promedio 2D por pixeles, no una "fusión" (morph) por landmarks faciales (ojos/nariz/boca/...).
        
*   **Paso 4: Reporte de calidad (`scripts/4_average_face_qc_report.Rmd`)**
    Convierte los manifiestos de control de calidad del paso anterior en un resumen visual en formato HTML para facilitar su revisión.
*   **Paso 5: Exportación (`scripts/5_export_album_bundle.R`)**
    Empaqueta los datos estructurados (JSON) y copia las imágenes seleccionadas para crear el archivo final listo para ser usado en otras aplicaciones.

## 🏛️ Fuente de los Datos y Transparencia

Toda la información se alimenta de datos públicos. Las páginas principales consultadas son:

- [JNE Voto Informado - Diputados](https://votoinformado.jne.gob.pe/diputados)
- [JNE Voto Informado - Presidente y Vicepresidentes](https://votoinformado.jne.gob.pe/presidente-vicepresidentes)
- [JNE Voto Informado - Senadores](https://votoinformado.jne.gob.pe/senadores)
- [JNE DNE - Voto Informado](https://dne.jne.gob.pe/informacion-electoral/voto-informado)

## Notas Clave

- El scraper en vivo debe correrse visible, no headless, usando la configuracion Firefox ya incluida en el proyecto.
- Las candidaturas exportadas conservan el campo `type`, por ejemplo `Diputado`, `Senador`, `Presidente` y `Vicepresidente`.
- Las candidaturas de `presidente-vicepresidentes` y `senadores` usan `Peru` como ambito geografico.

**Aclaración:** Este es un proyecto de procesamiento de datos no oficial construido sobre información electoral pública. No estamos afiliados, respaldados ni mantenidos por el Jurado Nacional de Elecciones (JNE).

Se recomienda la siguiente atribución al publicar resultados derivados de este proyecto:

> *Fuente: Jurado Nacional de Elecciones (JNE), Voto Informado - Diputados, https://votoinformado.jne.gob.pe/diputados*
