# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze — ingesta incremental con Auto Loader
# MAGIC - Lee **solo los archivos nuevos** de cada carpeta del Volume (Auto Loader recuerda lo ya leído en su checkpoint).
# MAGIC - Guarda todo **como texto**: bronze es una copia fiel; los tipos y validaciones van en silver.
# MAGIC   Así un dato mal escrito no rompe la carga ni se pierde.
# MAGIC - Agrega metadatos de trazabilidad: archivo de origen y momento de ingesta.
# MAGIC - `trigger(availableNow=True)`: procesa lo pendiente y termina (modo batch incremental, ideal para un Job).

# COMMAND ----------

# ==== 1. PARÁMETROS ====
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")

ruta_archivos = f"/Volumes/{catalogo}/landing/archivos"
ruta_checkpoints = f"/Volumes/{catalogo}/landing/checkpoints"

# Columnas esperadas por fuente (todas STRING en bronze)
FUENTES = {
    "cosecha": ["id_registro", "fecha_cosecha", "codigo_lote", "turno", "kilos_exportables", "kilos_descarte", "fecha_carga"],
    "clima_horario": ["estacion", "fecha_hora", "temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation",
                      "vapour_pressure_deficit", "shortwave_radiation", "wind_speed_10m", "et0_fao_evapotranspiration"],
    "maestro_lotes": ["codigo_lote", "valle", "estacion_clima", "variedad", "area_ha"],
}
CARPETA_POR_FUENTE = {"cosecha": "cosecha", "clima_horario": "clima", "maestro_lotes": "maestro"}

# COMMAND ----------

# ==== 2. AUTO LOADER POR FUENTE ====
# Se usa 3 veces (una por fuente)
def ingerir_bronze(fuente):
    esquema = ", ".join(f"{c} STRING" for c in FUENTES[fuente])
    tabla = f"{catalogo}.bronze.{fuente}"

    consulta = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_datos_rescatados")   # columnas inesperadas no se pierden
        .schema(esquema)
        .load(f"{ruta_archivos}/{CARPETA_POR_FUENTE[fuente]}")
        .withColumn("_archivo_origen", F.col("_metadata.file_path"))
        .withColumn("_fecha_ingesta", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{ruta_checkpoints}/bronze_{fuente}")
        .trigger(availableNow=True)
        .toTable(tabla)
    )
    consulta.awaitTermination()

    filas_nuevas = sum(p["numInputRows"] for p in consulta.recentProgress)
    print(f"✅ {tabla}: {filas_nuevas:,} filas nuevas")

# COMMAND ----------

# ==== 3. EJECUCIÓN ====
for fuente in FUENTES:
    ingerir_bronze(fuente)

# COMMAND ----------

# ==== 4. CONTROL: archivos ingeridos por fuente ====
for fuente in FUENTES:
    display(
        spark.table(f"{catalogo}.bronze.{fuente}")
        .groupBy("_archivo_origen").count()
        .orderBy(F.desc("_archivo_origen")).limit(5)
    )
