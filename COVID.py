# Databricks notebook source
# MAGIC %md
# MAGIC # 🦠 Pipeline COVID-19 Argentina — Análisis a Escala con PySpark
# MAGIC
# MAGIC ## Sobre el dataset
# MAGIC
# MAGIC Este proyecto trabaja con el dataset consolidado de casos de COVID-19 de la República
# MAGIC Argentina, publicado por el Ministerio de Salud. No es un dataset cualquiera: es el
# MAGIC **archivo CSV monolítico más pesado jamás publicado en el portal de Datos Abiertos
# MAGIC del país** — un único bloque continuo (sin particionar por año o mes, a diferencia
# MAGIC de otras bases estatales) que llegó a superar ampliamente el millón de filas límite
# MAGIC de Excel, obligando al propio Estado a publicar una guía oficial para abrir archivos
# MAGIC de gran escala con herramientas como Python o R.
# MAGIC
# MAGIC La versión utilizada en este proyecto pesa **6,5 GB** y contiene **29.971.992 filas**
# MAGIC con 25 columnas — el registro completo, sin recortar, de todos los casos sospechosos,
# MAGIC confirmados y descartados notificados al Sistema Nacional de Vigilancia de la Salud
# MAGIC (SNVS) a lo largo de toda la pandemia.
# MAGIC
# MAGIC ## Por qué Spark, no pandas
# MAGIC
# MAGIC Un archivo de este volumen es intratable con pandas en una notebook local sin
# MAGIC particionar manualmente en chunks (como en la primera versión de este proyecto).
# MAGIC Acá se reconstruye el pipeline completo usando **PySpark sobre Databricks**, con
# MAGIC arquitectura Medallion (Bronze → Silver → Gold) y Unity Catalog, para procesar
# MAGIC el dataset completo de forma distribuida, sin trucos de memoria.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Ingesta cruda
# MAGIC
# MAGIC Cargamos el CSV original tal cual, sin inferir tipos de dato (`inferSchema = false`).
# MAGIC Todas las columnas se definen como `STRING` — Bronze es una copia fiel del dato crudo,
# MAGIC sin ninguna interpretación todavía. La tipificación real (enteros, fechas, booleanos)
# MAGIC se hace recién en la capa Silver, donde tenemos control explícito columna por columna.
# MAGIC
# MAGIC Fuente: `Covid19Casos.csv` (29.971.992 filas, 25 columnas), subido al Volume
# MAGIC `workspace.covid.raw_data` vía Databricks SDK.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.covid.bronze_casos (
# MAGIC   id_evento_caso STRING,
# MAGIC   sexo STRING,
# MAGIC   edad STRING,
# MAGIC   `edad_años_meses` STRING,
# MAGIC   residencia_pais_nombre STRING,
# MAGIC   residencia_provincia_nombre STRING,
# MAGIC   residencia_departamento_nombre STRING,
# MAGIC   carga_provincia_nombre STRING,
# MAGIC   fecha_inicio_sintomas STRING,
# MAGIC   fecha_apertura STRING,
# MAGIC   sepi_apertura STRING,
# MAGIC   fecha_internacion STRING,
# MAGIC   cuidado_intensivo STRING,
# MAGIC   fecha_cui_intensivo STRING,
# MAGIC   fallecido STRING,
# MAGIC   fecha_fallecimiento STRING,
# MAGIC   asistencia_respiratoria_mecanica STRING,
# MAGIC   carga_provincia_id STRING,
# MAGIC   origen_financiamiento STRING,
# MAGIC   clasificacion STRING,
# MAGIC   clasificacion_resumen STRING,
# MAGIC   residencia_provincia_id STRING,
# MAGIC   fecha_diagnostico STRING,
# MAGIC   residencia_departamento_id STRING,
# MAGIC   ultima_actualizacion STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga incremental con COPY INTO
# MAGIC
# MAGIC `COPY INTO` es idempotente por diseño: si se vuelve a correr, Databricks lleva registro
# MAGIC de qué archivos ya procesó y no los vuelve a cargar, evitando duplicados. Es la forma
# MAGIC recomendada de ingesta en Unity Catalog para este tipo de carga masiva desde un Volume.

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO workspace.covid.bronze_casos
# MAGIC FROM '/Volumes/workspace/covid/raw_data/Covid19Casos.csv'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'false')

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA rápido sobre Bronze
# MAGIC
# MAGIC Antes de definir el schema tipado de Silver, revisamos cada columna: cardinalidad
# MAGIC (valores distintos) y proporción de nulos. Esto nos permite detectar columnas
# MAGIC "basura" (por ejemplo, con un solo valor constante tipo "missing value" en el
# MAGIC 100% de las filas) antes de invertir esfuerzo tipificándolas.

# COMMAND ----------

from pyspark.sql.functions import col, count, when

df_bronze = spark.table("workspace.covid.bronze_casos")
total_rows = df_bronze.count()

print(f"Total de filas: {total_rows:,}\n")

for column in df_bronze.columns:
    distinct_count = df_bronze.select(column).distinct().count()
    null_count = df_bronze.filter(
        col(column).isNull() | (col(column) == "")
    ).count()
    null_pct = (null_count / total_rows) * 100

    print(f"{column:35s} | valores distintos: {distinct_count:>10,} | nulos/vacíos: {null_count:>10,} ({null_pct:5.1f}%)")

# COMMAND ----------

from pyspark.sql.functions import col

columnas_categoricas = [
    "sexo",
    "edad_años_meses",
    "cuidado_intensivo",
    "fallecido",
    "asistencia_respiratoria_mecanica",
    "origen_financiamiento",
    "clasificacion_resumen"
]

for c in columnas_categoricas:
    print(f"\n===== {c} =====")
    (
        df_bronze
        .groupBy(c)
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Limpieza y tipado
# MAGIC
# MAGIC Se aplican las siguientes transformaciones sobre Bronze:
# MAGIC
# MAGIC - **Fechas** (`fecha_inicio_sintomas`, `fecha_apertura`, `fecha_internacion`, `fecha_cui_intensivo`,
# MAGIC   `fecha_fallecimiento`, `fecha_diagnostico`): convertidas de STRING a DATE (formato ISO `yyyy-MM-dd`).
# MAGIC - **Booleanas** (`cuidado_intensivo`, `fallecido`, `asistencia_respiratoria_mecanica`): "SI"/"NO" → BOOLEAN.
# MAGIC - **`edad`**: normalizada a `edad_anios` (INT). Los valores del grupo "Meses" se mantienen en 0
# MAGIC   (bebé menor de 1 año). Los valores negativos, mayores a 120, o nulos dentro del grupo "Años"
# MAGIC   se convierten a NULL (0.037% de los casos — ver EDA previo).
# MAGIC - **`origen_financiamiento`**: el placeholder `*sin dato*` se convierte a NULL.
# MAGIC - **`sexo`**: se mantiene "NR" como categoría propia (no se nulea) — representa una respuesta
# MAGIC   real de "no registrado", distinta de un fallo de carga.
# MAGIC - Columnas de solo metadato del archivo (`ultima_actualizacion`) se descartan, ya que son
# MAGIC   constantes y no aportan información analítica.

# COMMAND ----------

from pyspark.sql.functions import col, when, to_date

df_silver = (
    df_bronze
    .withColumn(
        "edad_anios",
        when(col("edad_años_meses") == "Meses", 0)
        .when(
            (col("edad_años_meses") == "Años")
            & col("edad").cast("int").isNotNull()
            & (col("edad").cast("int") >= 0)
            & (col("edad").cast("int") <= 120),
            col("edad").cast("int")
        )
        .otherwise(None)
    )
    .withColumn("fecha_inicio_sintomas", to_date(col("fecha_inicio_sintomas"), "yyyy-MM-dd"))
    .withColumn("fecha_apertura", to_date(col("fecha_apertura"), "yyyy-MM-dd"))
    .withColumn("fecha_internacion", to_date(col("fecha_internacion"), "yyyy-MM-dd"))
    .withColumn("fecha_cui_intensivo", to_date(col("fecha_cui_intensivo"), "yyyy-MM-dd"))
    .withColumn("fecha_fallecimiento", to_date(col("fecha_fallecimiento"), "yyyy-MM-dd"))
    .withColumn("fecha_diagnostico", to_date(col("fecha_diagnostico"), "yyyy-MM-dd"))
    .withColumn("cuidado_intensivo", when(col("cuidado_intensivo") == "SI", True).otherwise(False))
    .withColumn("fallecido", when(col("fallecido") == "SI", True).otherwise(False))
    .withColumn(
        "asistencia_respiratoria_mecanica",
        when(col("asistencia_respiratoria_mecanica") == "SI", True).otherwise(False)
    )
    .withColumn(
        "origen_financiamiento",
        when(col("origen_financiamiento") == "*sin dato*", None).otherwise(col("origen_financiamiento"))
    )
    .drop("edad", "edad_años_meses", "ultima_actualizacion")
)

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.covid.silver_casos")
)

print("Silver creada con éxito.")
print(f"Filas: {spark.table('workspace.covid.silver_casos').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deduplicación de id_evento_caso
# MAGIC
# MAGIC Se detectaron 6.398 id_evento_caso con exactamente 2 filas idénticas en todas las
# MAGIC columnas excepto `origen_financiamiento` (Público vs. Privado) — probablemente el
# MAGIC mismo evento reportado por dos subsistemas de cobertura. Se conserva una sola fila
# MAGIC por id_evento_caso (criterio determinístico: se prioriza "Público" cuando ambas
# MAGIC existen, dado que es el valor mayoritario en el dataset completo).

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number, when

window_dedup = Window.partitionBy("id_evento_caso").orderBy(
    when(col("origen_financiamiento") == "Público", 0).otherwise(1)
)

df_silver_dedup = (
    df_silver
    .withColumn("rn", row_number().over(window_dedup))
    .filter(col("rn") == 1)
    .drop("rn")
)

print(f"Filas antes: {df_silver.count():,}")
print(f"Filas después de deduplicar: {df_silver_dedup.count():,}")

(
    df_silver_dedup.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.covid.silver_casos")
)

print("Silver actualizada sin duplicados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renombrado de columnas para legibilidad
# MAGIC
# MAGIC Se renombran columnas técnicas a nombres más claros para analistas de negocio,
# MAGIC siguiendo el criterio de que Silver debe representar el dominio de forma legible,
# MAGIC no solo técnicamente correcta.

# COMMAND ----------

df_silver_final = (
    df_silver_dedup
    .withColumnRenamed("id_evento_caso", "id_caso")
    .withColumnRenamed("residencia_provincia_nombre", "provincia")
    .withColumnRenamed("residencia_departamento_nombre", "departamento")
    .withColumnRenamed("residencia_pais_nombre", "pais")
    .withColumnRenamed("asistencia_respiratoria_mecanica", "requirio_arm")
    .withColumnRenamed("cuidado_intensivo", "requirio_uti")
    .withColumnRenamed("clasificacion", "clasificacion_detalle")
    .withColumnRenamed("clasificacion_resumen", "clasificacion")
)

(
    df_silver_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.covid.silver_casos")
)

print("Silver renombrada y guardada.")
print(f"Columnas finales: {df_silver_final.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Dimensiones geográficas
# MAGIC
# MAGIC `dim_provincia` usa `provincia_id` (código INDEC) como clave — es único a nivel
# MAGIC nacional, sin ambigüedad.
# MAGIC
# MAGIC `dim_departamento` **no puede** usar el código INDEC de departamento como parte de
# MAGIC su clave: se detectaron 23 combinaciones de (departamento_id, provincia_id) que
# MAGIC mapean a más de un nombre de departamento distinto (ej: código "035" en provincia "06"
# MAGIC representa a la vez "Avellaneda", "General Pedernera" y "Junín" — departamentos sin
# MAGIC relación entre sí). Esto afecta 1.293.221 filas (4.3% del dataset) y es un problema
# MAGIC real del dato de origen, no de formato.
# MAGIC
# MAGIC Se resuelve usando **(`departamento_nombre`, `provincia_id`)** como clave de
# MAGIC `dim_departamento`, con `departamento_sk` como clave sustituta para los joins.
# MAGIC El código INDEC original se conserva como columna informativa, pero no se usa
# MAGIC para el join.

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id, first

df_silver = spark.table("workspace.covid.silver_casos")

dim_departamento = (
    df_silver
    .filter(col("departamento").isNotNull())
    .groupBy(
        col("departamento").alias("departamento_nombre"),
        col("residencia_provincia_id").alias("provincia_id")
    )
    .agg(
        first("residencia_departamento_id", ignorenulls=True).alias("departamento_id_original")
    )
    .withColumn("departamento_sk", monotonically_increasing_id())
)

dim_departamento.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.dim_departamento")

print(f"dim_departamento: {dim_departamento.count()} filas")

chequeo = (
    dim_departamento.groupBy("departamento_nombre", "provincia_id")
    .count()
    .filter(col("count") > 1)
)
print(f"Combinaciones (nombre, provincia) duplicadas: {chequeo.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: fact_casos
# MAGIC
# MAGIC Tabla de hechos a nivel de caso individual. Referencia a las dimensiones por
# MAGIC `provincia_id` (clave nacional única) y `departamento_sk` (clave sustituta,
# MAGIC dado que el código INDEC de departamento se repite entre provincias).
# MAGIC
# MAGIC Se agregan columnas derivadas de demora (en días) entre eventos clínicos clave:
# MAGIC inicio de síntomas → diagnóstico, internación, UTI, fallecimiento, y notificación
# MAGIC administrativa (apertura del caso).

# COMMAND ----------

from pyspark.sql.functions import col, datediff

df_silver = spark.table("workspace.covid.silver_casos")
dim_departamento = spark.table("workspace.covid.dim_departamento")

fact_casos = (
    df_silver
    .join(
        dim_departamento.select("departamento_nombre", "provincia_id", "departamento_sk"),
        (df_silver["departamento"] == dim_departamento["departamento_nombre"])
        & (df_silver["residencia_provincia_id"] == dim_departamento["provincia_id"]),
        "left"
    )
    .select(
        col("id_caso"),
        col("sexo"),
        col("pais"),
        col("residencia_provincia_id").alias("provincia_id"),
        col("departamento_sk"),
        col("edad_anios"),
        col("clasificacion"),
        col("origen_financiamiento"),
        col("requirio_uti"),
        col("requirio_arm"),
        col("fallecido"),
        col("fecha_inicio_sintomas"),
        col("fecha_apertura"),
        col("fecha_diagnostico"),
        col("fecha_internacion"),
        col("fecha_cui_intensivo"),
        col("fecha_fallecimiento"),
        datediff(col("fecha_apertura"), col("fecha_inicio_sintomas")).alias("dias_a_notificacion"),
        datediff(col("fecha_diagnostico"), col("fecha_inicio_sintomas")).alias("dias_a_diagnostico"),
        datediff(col("fecha_internacion"), col("fecha_inicio_sintomas")).alias("dias_a_internacion"),
        datediff(col("fecha_cui_intensivo"), col("fecha_inicio_sintomas")).alias("dias_a_uti"),
        datediff(col("fecha_fallecimiento"), col("fecha_inicio_sintomas")).alias("dias_a_fallecimiento"),
    )
)

fact_casos.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.fact_casos")

print(f"fact_casos: {fact_casos.count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación de nulos en fact_casos
# MAGIC
# MAGIC Chequeo de calidad post-join: confirmar que las columnas clave (especialmente
# MAGIC `departamento_sk`, resultado del join con la dimensión) no tengan una proporción
# MAGIC inesperada de nulos.

# COMMAND ----------

from pyspark.sql.functions import col

fact_casos = spark.table("workspace.covid.fact_casos")
total = fact_casos.count()

for column in fact_casos.columns:
    nulls = fact_casos.filter(col(column).isNull()).count()
    if nulls > 0:
        print(f"{column:25s} | nulos: {nulls:>10,} ({nulls/total*100:5.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_mortalidad_por_sexo
# MAGIC
# MAGIC Tasa de mortalidad y severidad (UTI/ARM) comparada por sexo, solo sobre casos
# MAGIC confirmados (el resto no tiene sentido para medir letalidad real).

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, round as spark_round

fact_casos = spark.table("workspace.covid.fact_casos")

gold_mortalidad_por_sexo = (
    fact_casos
    .filter(col("clasificacion") == "Confirmado")
    .groupBy("sexo")
    .agg(
        count("*").alias("total_casos"),
        spark_sum(col("fallecido").cast("int")).alias("total_fallecidos"),
        spark_sum(col("requirio_uti").cast("int")).alias("total_uti"),
        spark_sum(col("requirio_arm").cast("int")).alias("total_arm"),
    )
    .withColumn("tasa_mortalidad_pct", spark_round(col("total_fallecidos") / col("total_casos") * 100, 2))
    .withColumn("tasa_uti_pct", spark_round(col("total_uti") / col("total_casos") * 100, 2))
    .withColumn("tasa_arm_pct", spark_round(col("total_arm") / col("total_casos") * 100, 2))
)

gold_mortalidad_por_sexo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_mortalidad_por_sexo")

gold_mortalidad_por_sexo.orderBy(col("total_casos").desc()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_casos_por_pais
# MAGIC
# MAGIC Cantidad de casos registrados por país de residencia, para identificar el volumen
# MAGIC de casos correspondientes a residentes extranjeros vs. Argentina.

# COMMAND ----------

gold_casos_por_pais = (
    fact_casos
    .groupBy("pais")
    .agg(
        count("*").alias("total_casos"),
        spark_sum(col("fallecido").cast("int")).alias("total_fallecidos"),
    )
    .withColumn(
        "pct_del_total",
        spark_round(col("total_casos") / fact_casos.count() * 100, 4)
    )
)

gold_casos_por_pais.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_casos_por_pais")

gold_casos_por_pais.orderBy(col("total_casos").desc()).show(120, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_kpis_generales
# MAGIC
# MAGIC Métricas resumen de una sola fila — el panorama general del dataset completo,
# MAGIC sin ningún cruce por dimensión. Sirve como "tarjetas" (cards) en el dashboard final.

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, avg, round as spark_round, lit

fact_casos = spark.table("workspace.covid.fact_casos")

total_casos = fact_casos.count()
confirmados = fact_casos.filter(col("clasificacion") == "Confirmado")
total_confirmados = confirmados.count()

gold_kpis_generales = spark.createDataFrame([{
    "total_casos_notificados": total_casos,
    "total_confirmados": total_confirmados,
    "total_descartados": fact_casos.filter(col("clasificacion") == "Descartado").count(),
    "total_fallecidos": confirmados.filter(col("fallecido")).count(),
    "total_requirio_uti": confirmados.filter(col("requirio_uti")).count(),
    "total_requirio_arm": confirmados.filter(col("requirio_arm")).count(),
    "tasa_mortalidad_pct": round(confirmados.filter(col("fallecido")).count() / total_confirmados * 100, 2),
    "promedio_dias_a_diagnostico": confirmados.agg(avg("dias_a_diagnostico")).collect()[0][0],
    "promedio_dias_a_fallecimiento": confirmados.agg(avg("dias_a_fallecimiento")).collect()[0][0],
}])

gold_kpis_generales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_kpis_generales")

gold_kpis_generales.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_casos_por_dia
# MAGIC
# MAGIC Serie temporal de casos confirmados por fecha de inicio de síntomas — la curva
# MAGIC epidemiológica clásica.

# COMMAND ----------

gold_casos_por_dia = (
    fact_casos
    .filter(col("clasificacion") == "Confirmado")
    .filter(col("fecha_inicio_sintomas").isNotNull())
    .groupBy("fecha_inicio_sintomas")
    .agg(
        count("*").alias("total_casos"),
        spark_sum(col("fallecido").cast("int")).alias("total_fallecidos"),
        spark_sum(col("requirio_uti").cast("int")).alias("total_uti"),
    )
    .orderBy("fecha_inicio_sintomas")
)

gold_casos_por_dia.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_casos_por_dia")

print(f"Días con datos: {gold_casos_por_dia.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_resumen_provincia
# MAGIC
# MAGIC Métricas de mortalidad y severidad por provincia, sobre casos confirmados.

# COMMAND ----------

dim_provincia = spark.table("workspace.covid.dim_provincia")

gold_resumen_provincia = (
    fact_casos
    .filter(col("clasificacion") == "Confirmado")
    .join(dim_provincia, fact_casos["provincia_id"] == dim_provincia["provincia_id"], "left")
    .groupBy(dim_provincia["provincia_nombre"])
    .agg(
        count("*").alias("total_casos"),
        spark_sum(col("fallecido").cast("int")).alias("total_fallecidos"),
        spark_sum(col("requirio_uti").cast("int")).alias("total_uti"),
        spark_sum(col("requirio_arm").cast("int")).alias("total_arm"),
        spark_round(avg("dias_a_diagnostico"), 1).alias("promedio_dias_diagnostico"),
    )
    .withColumn("tasa_mortalidad_pct", spark_round(col("total_fallecidos") / col("total_casos") * 100, 2))
)

gold_resumen_provincia.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_resumen_provincia")

gold_resumen_provincia.orderBy(col("total_casos").desc()).show(25, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_mortalidad_por_edad
# MAGIC
# MAGIC Letalidad y severidad por franja etaria — la curva clásica que muestra cómo
# MAGIC aumenta el riesgo con la edad.

# COMMAND ----------

from pyspark.sql.functions import when as spark_when

gold_mortalidad_por_edad = (
    fact_casos
    .filter(col("clasificacion") == "Confirmado")
    .filter(col("edad_anios").isNotNull())
    .withColumn(
        "franja_etaria",
        spark_when(col("edad_anios") < 10, "0-9")
        .when(col("edad_anios") < 20, "10-19")
        .when(col("edad_anios") < 30, "20-29")
        .when(col("edad_anios") < 40, "30-39")
        .when(col("edad_anios") < 50, "40-49")
        .when(col("edad_anios") < 60, "50-59")
        .when(col("edad_anios") < 70, "60-69")
        .when(col("edad_anios") < 80, "70-79")
        .otherwise("80+")
    )
    .groupBy("franja_etaria")
    .agg(
        count("*").alias("total_casos"),
        spark_sum(col("fallecido").cast("int")).alias("total_fallecidos"),
        spark_sum(col("requirio_uti").cast("int")).alias("total_uti"),
    )
    .withColumn("tasa_mortalidad_pct", spark_round(col("total_fallecidos") / col("total_casos") * 100, 2))
    .orderBy("franja_etaria")
)

gold_mortalidad_por_edad.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_mortalidad_por_edad")

gold_mortalidad_por_edad.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_demoras_sistema_salud
# MAGIC
# MAGIC Promedio de días de demora (notificación, diagnóstico, internación) por provincia
# MAGIC — indicador de capacidad de respuesta del sistema sanitario.

# COMMAND ----------

gold_demoras_sistema_salud = (
    fact_casos
    .filter(col("clasificacion") == "Confirmado")
    .join(dim_provincia, fact_casos["provincia_id"] == dim_provincia["provincia_id"], "left")
    .groupBy(dim_provincia["provincia_nombre"])
    .agg(
        spark_round(avg("dias_a_notificacion"), 1).alias("promedio_dias_notificacion"),
        spark_round(avg("dias_a_diagnostico"), 1).alias("promedio_dias_diagnostico"),
        spark_round(avg("dias_a_internacion"), 1).alias("promedio_dias_internacion"),
    )
    .orderBy("provincia_nombre")
)

gold_demoras_sistema_salud.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_demoras_sistema_salud")

gold_demoras_sistema_salud.show(25, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_publico_vs_privado
# MAGIC
# MAGIC Comparación de severidad y mortalidad según origen de financiamiento.

# COMMAND ----------

gold_publico_vs_privado = (
    fact_casos
    .filter(col("clasificacion") == "Confirmado")
    .filter(col("origen_financiamiento").isNotNull())
    .groupBy("origen_financiamiento")
    .agg(
        count("*").alias("total_casos"),
        spark_sum(col("fallecido").cast("int")).alias("total_fallecidos"),
        spark_sum(col("requirio_uti").cast("int")).alias("total_uti"),
    )
    .withColumn("tasa_mortalidad_pct", spark_round(col("total_fallecidos") / col("total_casos") * 100, 2))
)

gold_publico_vs_privado.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_publico_vs_privado")

gold_publico_vs_privado.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_efectividad_uti / gold_efectividad_arm
# MAGIC
# MAGIC Compara la tasa de mortalidad entre quienes requirieron UTI (o ARM) y quienes no,
# MAGIC sobre casos confirmados. Sirve para dimensionar el perfil de gravedad asociado a
# MAGIC cada intervención — no mide "efectividad del tratamiento" en sentido clínico
# MAGIC estricto, ya que quienes llegan a UTI/ARM ya son, por definición, los casos más
# MAGIC graves (hay un sesgo de selección esperable).

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, round as spark_round

confirmados = fact_casos.filter(col("clasificacion") == "Confirmado")

gold_efectividad_uti = (
    confirmados.groupBy("requirio_uti")
    .agg(
        count("*").alias("total_casos"),
        spark_sum(col("fallecido").cast("int")).alias("total_fallecidos"),
    )
    .withColumn("tasa_mortalidad_pct", spark_round(col("total_fallecidos") / col("total_casos") * 100, 2))
)

gold_efectividad_arm = (
    confirmados.groupBy("requirio_arm")
    .agg(
        count("*").alias("total_casos"),
        spark_sum(col("fallecido").cast("int")).alias("total_fallecidos"),
    )
    .withColumn("tasa_mortalidad_pct", spark_round(col("total_fallecidos") / col("total_casos") * 100, 2))
)

gold_efectividad_uti.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_efectividad_uti")
gold_efectividad_arm.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_efectividad_arm")

gold_efectividad_uti.show()
gold_efectividad_arm.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_composicion_fallecidos_sexo
# MAGIC
# MAGIC De los fallecidos totales (no del total de casos confirmados), cómo se reparten
# MAGIC por sexo — mirada de **composición**, distinta a la tasa de mortalidad por sexo
# MAGIC ya calculada en `gold_mortalidad_por_sexo`. Responde "¿quiénes son los que
# MAGIC murieron?" en vez de "¿qué tan riesgoso es cada grupo?".

# COMMAND ----------

gold_composicion_fallecidos_sexo = (
    confirmados.filter(col("fallecido"))
    .groupBy("sexo")
    .agg(count("*").alias("total_fallecidos"))
)

total_fallecidos_general = gold_composicion_fallecidos_sexo.agg(spark_sum("total_fallecidos")).collect()[0][0]

gold_composicion_fallecidos_sexo = gold_composicion_fallecidos_sexo.withColumn(
    "pct_del_total", spark_round(col("total_fallecidos") / total_fallecidos_general * 100, 1)
)

gold_composicion_fallecidos_sexo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.covid.gold_composicion_fallecidos_sexo")
gold_composicion_fallecidos_sexo.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualización: Curva epidemiológica
# MAGIC
# MAGIC Casos confirmados agrupados por fecha de inicio de síntomas — muestra la evolución
# MAGIC de la pandemia a lo largo del tiempo, con sus distintas olas.

# COMMAND ----------

import matplotlib.pyplot as plt

pdf_casos_dia = spark.table("workspace.covid.gold_casos_por_dia").toPandas()
pdf_casos_dia = pdf_casos_dia.sort_values("fecha_inicio_sintomas")

plt.figure(figsize=(14, 5))
plt.plot(pdf_casos_dia["fecha_inicio_sintomas"], pdf_casos_dia["total_casos"], linewidth=1)
plt.title("Casos confirmados por fecha de inicio de síntomas")
plt.xlabel("Fecha")
plt.ylabel("Casos")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualización: Mortalidad por franja etaria
# MAGIC
# MAGIC La curva clásica de letalidad por COVID — el riesgo aumenta marcadamente con la edad.

# COMMAND ----------

pdf_edad = spark.table("workspace.covid.gold_mortalidad_por_edad").toPandas()

plt.figure(figsize=(10, 5))
plt.bar(pdf_edad["franja_etaria"], pdf_edad["tasa_mortalidad_pct"], color="#e67e22")
plt.title("Tasa de mortalidad por franja etaria (casos confirmados)")
plt.xlabel("Franja etaria")
plt.ylabel("Mortalidad (%)")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualización: Mortalidad por provincia
# MAGIC
# MAGIC Comparación de la tasa de letalidad entre provincias, ordenadas de mayor a menor.

# COMMAND ----------

pdf_provincia = spark.table("workspace.covid.gold_resumen_provincia").toPandas()
pdf_provincia = pdf_provincia.sort_values("tasa_mortalidad_pct", ascending=True)

plt.figure(figsize=(10, 8))
plt.barh(pdf_provincia["provincia_nombre"], pdf_provincia["tasa_mortalidad_pct"], color="#9b59b6")
plt.title("Tasa de mortalidad por provincia (casos confirmados)")
plt.xlabel("Mortalidad (%)")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualización: Demora promedio a diagnóstico por provincia
# MAGIC
# MAGIC Días promedio entre el inicio de síntomas y el diagnóstico — un indicador de
# MAGIC capacidad de testeo por jurisdicción.

# COMMAND ----------

pdf_demoras = spark.table("workspace.covid.gold_demoras_sistema_salud").toPandas()
pdf_demoras = pdf_demoras.sort_values("promedio_dias_diagnostico", ascending=True)

plt.figure(figsize=(10, 8))
plt.barh(pdf_demoras["provincia_nombre"], pdf_demoras["promedio_dias_diagnostico"], color="#16a085")
plt.title("Demora promedio a diagnóstico por provincia (días)")
plt.xlabel("Días promedio")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualización: Público vs. Privado
# MAGIC
# MAGIC Comparación de severidad y mortalidad según el tipo de cobertura de salud.

# COMMAND ----------

pdf_financiamiento = spark.table("workspace.covid.gold_publico_vs_privado").toPandas()

fig, axes = plt.subplots(1, 3, figsize=(15, 5))
metricas = ["tasa_mortalidad_pct", "total_uti", "total_casos"]
titulos = ["Mortalidad (%)", "Casos en UTI", "Total de casos"]

for ax, metrica, titulo in zip(axes, metricas, titulos):
    ax.bar(pdf_financiamiento["origen_financiamiento"], pdf_financiamiento[metrica], color=["#3498db", "#e74c3c"])
    ax.set_title(titulo)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_edad_fallecidos
# MAGIC
# MAGIC Distribución de la cantidad de fallecidos por edad — muestra en qué rango etario
# MAGIC se concentra la mayor cantidad de muertes en términos absolutos.

# COMMAND ----------

import matplotlib.pyplot as plt

pdf_edad_fallecidos = spark.table("workspace.covid.gold_edad_fallecidos").toPandas()

plt.figure(figsize=(12, 5))
plt.bar(pdf_edad_fallecidos["edad_anios"], pdf_edad_fallecidos["total_fallecidos"], color="#c0392b", width=1)
plt.title("Distribución de fallecidos por edad")
plt.xlabel("Edad")
plt.ylabel("Cantidad de fallecidos")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_efectividad_uti_arm
# MAGIC
# MAGIC Compara la tasa de mortalidad entre quienes requirieron UTI/ARM y quienes no,
# MAGIC para dimensionar la gravedad asociada a cada intervención (no mide "efectividad
# MAGIC del tratamiento" en sí, sino el perfil de riesgo de quien llega a necesitarlo).

# COMMAND ----------

pdf_provincia = spark.table("workspace.covid.gold_resumen_provincia").toPandas()
pdf_provincia_top = pdf_provincia.sort_values("total_casos", ascending=True)

plt.figure(figsize=(10, 8))
plt.barh(pdf_provincia_top["provincia_nombre"], pdf_provincia_top["total_casos"], color="#2980b9")
plt.title("Provincias por cantidad de casos confirmados")
plt.xlabel("Casos confirmados")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: gold_composicion_fallecidos_sexo
# MAGIC
# MAGIC De los fallecidos totales, cómo se reparten por sexo (composición, no tasa).

# COMMAND ----------

pdf_kpis = spark.table("workspace.covid.gold_kpis_generales").toPandas()
no_fallecidos = pdf_kpis["total_confirmados"][0] - pdf_kpis["total_fallecidos"][0]

plt.figure(figsize=(6, 5))
plt.bar(["Fallecidos", "No fallecidos"], [pdf_kpis["total_fallecidos"][0], no_fallecidos], color=["#c0392b", "#27ae60"])
plt.title("Fallecidos vs. no fallecidos (casos confirmados)")
plt.ylabel("Cantidad de casos")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualización: Mortalidad según requirió UTI / ARM

# COMMAND ----------

pdf_uti = spark.table("workspace.covid.gold_efectividad_uti").toPandas()
pdf_arm = spark.table("workspace.covid.gold_efectividad_arm").toPandas()

fig, axes = plt.subplots(1, 2, figsize=(12, 5))

axes[0].bar(pdf_uti["requirio_uti"].astype(str), pdf_uti["tasa_mortalidad_pct"], color=["#95a5a6", "#e74c3c"])
axes[0].set_title("Mortalidad según requirió UTI")
axes[0].set_ylabel("Mortalidad (%)")

axes[1].bar(pdf_arm["requirio_arm"].astype(str), pdf_arm["tasa_mortalidad_pct"], color=["#95a5a6", "#e74c3c"])
axes[1].set_title("Mortalidad según requirió ARM")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualización: Composición de fallecidos por sexo

# COMMAND ----------

pdf_fallecidos_sexo = spark.table("workspace.covid.gold_composicion_fallecidos_sexo").toPandas()

plt.figure(figsize=(6, 6))
plt.pie(pdf_fallecidos_sexo["total_fallecidos"], labels=pdf_fallecidos_sexo["sexo"], autopct="%1.1f%%", colors=["#3498db", "#e74c3c", "#95a5a6"])
plt.title("Composición de fallecidos por sexo")
plt.show()