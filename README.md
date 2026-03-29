# 🗳️ Recopilación y Procesamiento de Datos: Elecciones Perú 2026


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
*   **Paso 4: Reporte de calidad (`scripts/4_average_face_qc_report.Rmd`)**
    Convierte los manifiestos de control de calidad del paso anterior en un resumen visual en formato HTML para facilitar su revisión.
*   **Paso 5: Exportación (`scripts/5_export_album_bundle.R`)**
    Empaqueta los datos estructurados (JSON) y copia las imágenes seleccionadas para crear el archivo final listo para ser usado en otras aplicaciones.

## 🏛️ Fuente de los Datos y Transparencia

Toda la información se alimenta de datos públicos. Las páginas principales consultadas son:

*   [JNE Voto Informado - Diputados](https://votoinformado.jne.gob.pe/diputados)
*   [JNE DNE - Voto Informado](https://dne.jne.gob.pe/informacion-electoral/voto-informado)

**Aclaración:** Este es un proyecto de procesamiento de datos no oficial construido sobre información electoral pública. No estamos afiliados, respaldados ni mantenidos por el Jurado Nacional de Elecciones (JNE).

Se recomienda la siguiente atribución al publicar resultados derivados de este proyecto:

> *Fuente: Jurado Nacional de Elecciones (JNE), Voto Informado - Diputados, https://votoinformado.jne.gob.pe/diputados*