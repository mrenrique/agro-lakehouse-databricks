# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gold — tablas analíticas
# MAGIC | Tabla | Grano | Uso |
# MAGIC |---|---|---|
# MAGIC | `gold.cosecha_clima_diaria` | lote × fecha | Features para modelos de rendimiento (kg/ha + clima acumulado) |
# MAGIC | `gold.kpi_semanal` | semana × valle × variedad | Dashboard de seguimiento de campaña |
# MAGIC
# MAGIC Son tablas chicas y derivadas: se reconstruyen completas en cada corrida (`overwrite`), lo que es
# MAGIC idempotente y evita errores de lógica incremental. La lógica incremental vive en silver.

# COMMAND ----------

# ==== 1. PARÁMETROS ====
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("inicio_campania", "2025-08-01")
catalogo = dbutils.widgets.get("catalogo")
inicio_campania = dbutils.widgets.get("inicio_campania")

# COMMAND ----------

# ==== 2. CLIMA CON VENTANAS MÓVILES ====
# Las ventanas se calculan sobre el clima DIARIO COMPLETO de cada estación, antes de unir con cosecha.
# Si se calcularan después del join, "7 días" serían 7 cosechas (que caen cada 3-4 días) y no 7 días reales.
ventana_7d = Window.partitionBy("estacion").orderBy("fecha").rowsBetween(-6, 0)
ventana_campania = Window.partitionBy("estacion").orderBy("fecha").rowsBetween(Window.unboundedPreceding, 0)

clima_features = (
    spark.table(f"{catalogo}.silver.clima_diario")
    .filter(F.col("fecha") >= F.date_sub(F.lit(inicio_campania).cast("date"), 30))   # margen para la primera ventana
    .withColumn("temp_media_7d", F.avg("temp_media").over(ventana_7d))
    .withColumn("horas_temp_mayor_30_7d", F.sum("horas_temp_mayor_30").over(ventana_7d))
    .withColumn("vpd_medio_7d", F.avg("vpd_medio").over(ventana_7d))
    .withColumn("lluvia_7d_mm", F.sum("lluvia_mm").over(ventana_7d))
    .withColumn("radiacion_7d_mj_m2", F.sum("radiacion_mj_m2").over(ventana_7d))
    .withColumn("gdd_acum_campania", F.sum(
        F.when(F.col("fecha") >= F.lit(inicio_campania).cast("date"), F.col("gdd_diario")).otherwise(0)
    ).over(ventana_campania))
    .select("estacion", "fecha", "dia_valido", "temp_media", "temp_max", "temp_min", "gdd_diario",
            "temp_media_7d", "horas_temp_mayor_30_7d", "vpd_medio_7d", "lluvia_7d_mm",
            "radiacion_7d_mj_m2", "gdd_acum_campania")
)

# COMMAND ----------

# ==== 3. GOLD: COSECHA × CLIMA A NIVEL LOTE-FECHA ====
ventana_lote = Window.partitionBy("codigo_lote").orderBy("fecha")

cosecha_clima = (
    spark.table(f"{catalogo}.silver.cosecha")
    # Un lote puede tener 2 registros el mismo día (turnos) → se agrega a lote × fecha
    .groupBy("codigo_lote", F.col("fecha_cosecha").alias("fecha"))
    .agg(F.sum("kilos_exportables").alias("kilos_exportables"),
         F.sum("kilos_descarte").alias("kilos_descarte"),
         F.count("*").alias("n_registros"))
    .join(spark.table(f"{catalogo}.silver.dim_lote"), "codigo_lote", "inner")
    .withColumn("kg_ha", F.col("kilos_exportables") / F.col("area_ha"))
    .withColumn("pct_descarte", F.col("kilos_descarte") / (F.col("kilos_exportables") + F.col("kilos_descarte")))
    .withColumn("dias_desde_pasada_anterior", F.datediff("fecha", F.lag("fecha").over(ventana_lote)))
    .withColumn("kg_ha_acum_campania", F.sum("kg_ha").over(ventana_lote.rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("semana_iso", F.weekofyear("fecha"))
    .join(clima_features.withColumnRenamed("estacion", "estacion_clima"), ["estacion_clima", "fecha"], "left")
)

cosecha_clima.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalogo}.gold.cosecha_clima_diaria")

sin_clima = cosecha_clima.filter(F.col("temp_media").isNull()).count()
print(f"✅ gold.cosecha_clima_diaria: {cosecha_clima.count():,} filas | sin clima: {sin_clima:,}")

# COMMAND ----------

# ==== 4. GOLD: KPI SEMANAL PARA DASHBOARD ====
# kg/ha se calcula como total de kilos / área total (no como promedio de kg/ha, que daría igual peso a lotes chicos)
kpi_semanal = (
    spark.table(f"{catalogo}.gold.cosecha_clima_diaria")
    .withColumn("semana_inicio", F.date_trunc("week", "fecha").cast("date"))
    .groupBy("semana_inicio", "semana_iso", "valle", "variedad")
    .agg(
        F.sum("kilos_exportables").alias("kilos_exportables"),
        F.countDistinct("codigo_lote").alias("lotes_cosechados"),
        F.avg("temp_media").alias("temp_media"),
        F.avg("pct_descarte").alias("pct_descarte_medio"),
    )
    .join(
        spark.table(f"{catalogo}.silver.dim_lote").groupBy("valle", "variedad").agg(F.sum("area_ha").alias("area_total_ha")),
        ["valle", "variedad"],
    )
    .withColumn("kg_ha", F.col("kilos_exportables") / F.col("area_total_ha"))
)
kpi_semanal.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalogo}.gold.kpi_semanal")
display(kpi_semanal.orderBy("semana_inicio", "valle", "variedad"))
