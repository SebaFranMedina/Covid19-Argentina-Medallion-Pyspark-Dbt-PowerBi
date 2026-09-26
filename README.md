# 🦠 Pipeline COVID-19 Argentina — Arquitectura Medallion con PySpark, dbt y Power BI

Pipeline de datos a escala construido sobre el dataset consolidado de casos de COVID-19 de la
República Argentina, publicado por el Ministerio de Salud — el **archivo CSV monolítico más
pesado jamás publicado en el portal de Datos Abiertos del país** (6,5 GB, ~30 millones de
filas, 25 columnas, sin particionar por año o mes).

## Sobre el dataset

El volumen del archivo obligó al propio Estado a publicar una guía oficial para abrirlo con
herramientas como Python o R, ya que supera ampliamente el límite de un millón de filas de
Excel. Es el registro completo, sin recortar, de todos los casos sospechosos, confirmados y
descartados notificados al Sistema Nacional de Vigilancia de la Salud (SNVS) a lo largo de
toda la pandemia.

## Por qué Spark, no pandas

Un archivo de este tamaño es intratable con pandas en un notebook local sin particionar
manualmente en chunks (como en la primera versión de este proyecto — ver sección
[Evolución del proyecto](#evolución-del-proyecto) más abajo). Este pipeline reconstruye el
proceso completo usando **PySpark sobre Databricks**, con arquitectura Medallion
(Bronze → Silver → Gold) y Unity Catalog, procesando el dataset completo de forma distribuida.

## Arquitectura

```
CSV (6,5 GB, 30M filas)
   │  Databricks SDK — chunked multipart upload
   ▼
Databricks Volume (workspace.covid.raw_data)
   │  COPY INTO (idempotente)
   ▼
🥉 Bronze — bronze_casos
   Copia cruda, todas las columnas STRING, sin inferSchema
   │  tipado, limpieza, deduplicación
   ▼
🥈 Silver — silver_casos
   Fechas a DATE, booleanos, edad derivada, dedup por id_caso
   │  star schema + métricas derivadas
   ▼
🥇 Gold — dim_provincia · dim_departamento · fact_casos · 16 tablas gold_* de resumen
   │
   ├──► Visualizaciones en el notebook (matplotlib)
   └──► Power BI
```

Silver y Gold están implementados **dos veces**, de forma independiente:

- **Notebook PySpark** (`COVID.py` / `COVID.ipynb`): el proceso original, con EDA, criterios de
  limpieza documentados en Markdown, y las visualizaciones en matplotlib.
- **Modelos dbt** (`models/*.sql`): la misma lógica de transformación migrada a SQL, versionada,
  testeada y documentada como código — corre sobre el mismo Serverless SQL Warehouse de
  Databricks Free Edition, en el schema de desarrollo `dbt_smedina`.

## Stack

| Capa | Herramienta |
|---|---|
| Almacenamiento / cómputo | Databricks Free Edition, Unity Catalog, Delta Lake |
| Ingesta | Databricks Volumes, `COPY INTO` |
| Transformación (v1) | PySpark |
| Transformación (v2) | dbt Cloud (dbt-databricks) |
| Visualización exploratoria | matplotlib |
| Dashboard | Power BI |
| Control de versiones | GitHub (Databricks Repos + dbt Cloud Git integration) |

## Capa Silver — criterios de limpieza

- **Fechas**: de STRING (ISO `yyyy-MM-dd`) a DATE.
- **Booleanos**: `cuidado_intensivo`, `fallecido`, `asistencia_respiratoria_mecanica` — "SI"/"NO" → BOOLEAN.
- **Edad**: normalizada a `edad_anios` (INT). Los casos con unidad "Meses" quedan en 0 (menores
  de 1 año); los valores negativos, mayores a 120 o nulos dentro de "Años" (0,037% de los casos)
  se convierten a NULL.
- **`origen_financiamiento`**: el placeholder `*sin dato*` se convierte a NULL.
- **`sexo`**: se mantiene "NR" como categoría propia — representa una respuesta real de "no
  registrado", no un fallo de carga.
- **Deduplicación**: 6.398 `id_evento_caso` con registros duplicados (idénticos salvo por
  `origen_financiamiento`) se resolvieron priorizando "Público" de forma determinística.

## Capa Gold — star schema

- `dim_provincia` (25 filas), `dim_departamento` (608 filas, clave compuesta
  departamento + provincia, ya que el código numérico oficial del Ministerio no es único a
  nivel nacional).
- `fact_casos`: un registro por caso, con métricas de demora derivadas
  (`dias_a_notificacion`, `dias_a_diagnostico`, `dias_a_internacion`, `dias_a_uti`,
  `dias_a_fallecimiento`).
- 16 tablas de resumen agregado para dashboarding: KPIs generales, curva epidemiológica,
  mortalidad por provincia/edad/sexo, letalidad mensual, mapa de calor casos por
  provincia y mes, pirámide de edad (casos vs. fallecidos), demoras del sistema de salud,
  público vs. privado, efectividad de UTI/ARM, entre otras.

## Calidad de datos (dbt tests)

`id_caso` verificado como único y no nulo en `silver_casos` y `fact_casos`; claves de
dimensión (`provincia_id`, `departamento`) también testeadas — todos los tests pasan.

## Algunos hallazgos

- **Salta** tiene la tasa de mortalidad más alta del país (~2,2%) pero un volumen de casos
  relativamente bajo — un patrón consistente con **menor capacidad de testeo** (se detectan
  sobre todo los casos graves, inflando la letalidad aparente) más que con una epidemia más
  grande.
- **La demora a diagnóstico, por sí sola, no explica** las diferencias de mortalidad entre
  provincias — provincias con demoras muy distintas (Misiones ~12 días, Córdoba ~3 días)
  tienen mortalidad similar.
- **Necesitar UTI o ARM ya es, en sí mismo, un indicador de gravedad extrema**: ~65-70% de
  mortalidad entre quienes requirieron UTI, ~80% entre quienes requirieron ARM — frente a
  mortalidad casi nula en quienes no los necesitaron.
- El grupo `sexo = "NR"` (no registrado) muestra una mortalidad notablemente más alta (4,71%)
  que M (1,65%) o F (1,13%) — hipótesis: los registros sin sexo cargado correlacionan con
  casos de alta urgencia, donde la carga administrativa fue apresurada.
- Los hallazgos de letalidad por sexo y edad son consistentes con los datos oficiales
  publicados por el Ministerio de Salud de Argentina.

## Evolución del proyecto

Este repo es la segunda versión del proyecto. La primera, construida con pandas y chunking
manual de 100k filas sobre 8 columnas, sigue disponible como referencia histórica en
[ANALISIS-DE-DATOS-CON-DATASET-DE-29-MILLONES-DE-DATOS](https://github.com/SebaFranMedina/ANALISIS-DE-DATOS-CON-DATASET-DE-29-MILLONES-DE-DATOS).

## Estructura del repo

```
├── COVID.ipynb              # Notebook PySpark: Bronze, EDA, Silver, Gold, visualizaciones
├── dbt_project.yml
└── models/
    ├── sources.yml           # Declaración de la fuente (bronze_casos)
    ├── silver_casos.sql
    ├── dim_provincia.sql
    ├── dim_departamento.sql
    ├── fact_casos.sql
    ├── gold_*.sql             # 16 modelos de resumen
    └── schema.yml             # Tests de calidad de datos
```
