# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver — clima diario agronómico por estación
# MAGIC Convierte el clima **horario** (Open-Meteo) en indicadores **diarios** útiles para cultivo:
# MAGIC cobertura de datos, temperaturas, horas de estrés térmico, humedad, VPD, radiación, lluvia, ET0 y grados día (GDD).
# MAGIC - Un día solo es `dia_valido` si tiene al menos 80% de las 24 horas esperadas.
# MAGIC - `MERGE` por `(estacion, fecha)` → re-ejecutar actualiza en vez de duplicar.

# COMMAND ----------

# ==== 1. PARÁMETROS AGRONÓMICOS ====
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")

TEMP_BASE_GDD = 7.0        # °C, base típica para arándano
VPD_ALTO = 1.2             # kPa
REGISTROS_ESPERADOS = 24   # 1 registro por hora
COBERTURA_MINIMA = 0.80

# COMMAND ----------

# ==== 2. CLIMA HORARIO TIPADO Y SIN DUPLICADOS ====
numericas = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation",
             "vapour_pressure_deficit", "shortwave_radiation", "wind_speed_10m", "et0_fao_evapotranspiration"]

clima_horario = (
    spark.table(f"{catalogo}.bronze.clima_horario")
    .withColumn("fecha_hora", F.expr("try_to_timestamp(fecha_hora, \"yyyy-MM-dd'T'HH:mm\")"))
    .select("estacion", "fecha_hora", "_fecha_ingesta",
            *[F.expr(f"try_cast({c} AS DOUBLE)").alias(c) for c in numericas])
    .filter(F.col("fecha_hora").isNotNull())
    # Si el mismo archivo se subió dos veces, se queda la ingesta más reciente
    .withColumn("fila", F.row_number().over(
        Window.partitionBy("estacion", "fecha_hora").orderBy(F.desc("_fecha_ingesta"))))
    .filter("fila = 1").drop("fila", "_fecha_ingesta")
    .withColumn("fecha", F.to_date("fecha_hora"))
)

# COMMAND ----------

# ==== 3. AGREGACIÓN DIARIA ====
es_dia = F.col("shortwave_radiation") > 5   # horas con luz solar
horas_si = lambda condicion: F.sum(F.when(condicion, 1).otherwise(0))   # cada registro = 1 hora

clima_diario = (
    clima_horario.groupBy("estacion", "fecha")
    .agg(
        # --- 3.1 Cobertura
        F.count("temperature_2m").alias("registros_temperatura"),
        # --- 3.2 Temperatura
        F.avg("temperature_2m").alias("temp_media"),
        F.max("temperature_2m").alias("temp_max"),
        F.min("temperature_2m").alias("temp_min"),
        F.avg(F.when(es_dia, F.col("temperature_2m"))).alias("temp_media_dia"),
        F.avg(F.when(~es_dia, F.col("temperature_2m"))).alias("temp_media_noche"),
        horas_si(F.col("temperature_2m") > 30).alias("horas_temp_mayor_30"),
        horas_si(F.col("temperature_2m") < 10).alias("horas_temp_menor_10"),
        # --- 3.3 Humedad y VPD
        F.avg("relative_humidity_2m").alias("hr_media"),
        horas_si(F.col("relative_humidity_2m") > 90).alias("horas_hr_mayor_90"),
        F.avg("vapour_pressure_deficit").alias("vpd_medio"),
        horas_si(F.col("vapour_pressure_deficit") > VPD_ALTO).alias("horas_vpd_alto"),
        # --- 3.4 Radiación, lluvia y evapotranspiración
        (F.sum("shortwave_radiation") * 3600 / 1e6).alias("radiacion_mj_m2"),   # W/m² por hora → MJ/m²/día
        horas_si(es_dia).alias("horas_luz"),
        F.sum("precipitation").alias("lluvia_mm"),
        F.sum("et0_fao_evapotranspiration").alias("et0_mm"),
    )
    .withColumn("cobertura", F.col("registros_temperatura") / REGISTROS_ESPERADOS)
    .withColumn("dia_valido", F.col("cobertura") >= COBERTURA_MINIMA)
    .withColumn("amplitud_termica", F.col("temp_max") - F.col("temp_min"))
    .withColumn("gdd_diario", F.greatest((F.col("temp_max") + F.col("temp_min")) / 2 - TEMP_BASE_GDD, F.lit(0.0)))
    .withColumn("balance_hidrico_mm", F.col("lluvia_mm") - F.col("et0_mm"))
    .withColumn("_fecha_proceso", F.current_timestamp())
)

# COMMAND ----------

# ==== 4. MERGE A SILVER ====
tabla = f"{catalogo}.silver.clima_diario"
if not spark.catalog.tableExists(tabla):
    clima_diario.limit(0).write.saveAsTable(tabla)   # crea la tabla con el esquema correcto

(
    DeltaTable.forName(spark, tabla).alias("t")
    .merge(clima_diario.alias("s"), "t.estacion = s.estacion AND t.fecha = s.fecha")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

resumen = spark.table(tabla).groupBy("estacion").agg(
    F.count("*").alias("dias"), F.min("fecha").alias("desde"), F.max("fecha").alias("hasta"),
    F.round(F.avg(F.col("dia_valido").cast("int")), 3).alias("pct_dias_validos"),
)
display(resumen)
