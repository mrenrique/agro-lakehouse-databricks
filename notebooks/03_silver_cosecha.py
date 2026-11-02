# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver — cosecha validada, deduplicada e idempotente
# MAGIC **Patrón:** stream desde bronze + `foreachBatch` + `MERGE`.
# MAGIC - El stream (con checkpoint) entrega solo las filas de bronze que aún no se procesaron → **incremental**.
# MAGIC - Dentro de cada lote se tipan los datos, se aplican reglas de calidad y se separan válidos de rechazados.
# MAGIC - `MERGE` por `id_registro`: si un registro llega dos veces, se queda la versión con `fecha_carga` más reciente.
# MAGIC   Re-ejecutar no duplica nada → **idempotente**.
# MAGIC - Los rechazados van a **cuarentena** con su motivo (no se borran: se pueden revisar y reprocesar).
# MAGIC - Cada lote deja una fila en `log_calidad` (cuántos llegaron, cuántos pasaron, por qué se rechazaron).

# COMMAND ----------

# ==== 1. PARÁMETROS Y REGLAS ====
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")
ruta_checkpoints = f"/Volumes/{catalogo}/landing/checkpoints"

KG_HA_MAX_REGISTRO = 2500.0   # más de esto en un solo registro es error de digitación

# COMMAND ----------

# ==== 2. DIMENSIÓN DE LOTES ====
# Tabla chica: se reconstruye completa en cada corrida (overwrite = idempotente)
dim_lote = (
    spark.table(f"{catalogo}.bronze.maestro_lotes")
    .withColumn("fila", F.row_number().over(Window.partitionBy("codigo_lote").orderBy(F.desc("_fecha_ingesta"))))
    .filter("fila = 1")
    .select(
        F.trim("codigo_lote").alias("codigo_lote"),
        "valle", "estacion_clima", "variedad",
        F.expr("try_cast(area_ha AS DOUBLE)").alias("area_ha"),
    )
)
dim_lote.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalogo}.silver.dim_lote")
print(f"✅ silver.dim_lote: {dim_lote.count()} lotes")

# COMMAND ----------

# ==== 3. TABLAS DESTINO (se crean una vez) ====
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.silver.cosecha (
    id_registro STRING, fecha_cosecha DATE, codigo_lote STRING, turno STRING,
    kilos_exportables DOUBLE, kilos_descarte DOUBLE, fecha_carga TIMESTAMP,
    _archivo_origen STRING, _fecha_proceso TIMESTAMP
) USING DELTA""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.silver.cosecha_cuarentena (
    id_registro STRING, fecha_cosecha_original STRING, codigo_lote STRING,
    kilos_exportables_original STRING, motivo_rechazo STRING,
    _archivo_origen STRING, _fecha_proceso TIMESTAMP
) USING DELTA""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.silver.log_calidad (
    tabla STRING, batch_id BIGINT, fecha_proceso TIMESTAMP,
    filas_recibidas BIGINT, filas_validas BIGINT, filas_rechazadas BIGINT,
    duplicados_en_lote BIGINT, rechazos_por_motivo MAP<STRING, BIGINT>
) USING DELTA""")

# COMMAND ----------

# ==== 4. PROCESAMIENTO DE CADA LOTE DEL STREAM ====
def procesar_lote(df_batch, batch_id):
    sesion = df_batch.sparkSession   # en foreachBatch se usa la sesión del lote, no la global
    lotes_validos = sesion.table(f"{catalogo}.silver.dim_lote").select("codigo_lote", "area_ha")

    # --- 4.1 Tipado tolerante: try_* devuelve NULL en vez de fallar si el texto no es válido
    df = (
        df_batch
        .withColumn("codigo_lote", F.trim("codigo_lote"))
        .withColumn("fecha_cosecha_dt", F.to_date(F.coalesce(
            F.expr("try_to_timestamp(fecha_cosecha, 'yyyy-MM-dd')"),
            F.expr("try_to_timestamp(fecha_cosecha, 'dd/MM/yyyy')"),   # formato alterno que llega a veces
        )))
        .withColumn("kilos_exp", F.expr("try_cast(kilos_exportables AS DOUBLE)"))
        .withColumn("kilos_desc", F.expr("try_cast(kilos_descarte AS DOUBLE)"))
        .withColumn("fecha_carga_ts", F.expr("try_to_timestamp(fecha_carga, 'yyyy-MM-dd HH:mm:ss')"))
    )

    # --- 4.2 Deduplicar dentro del lote: misma clave → la carga más reciente
    n_recibidas = df.count()
    df = (
        df.withColumn("fila", F.row_number().over(
            Window.partitionBy("id_registro").orderBy(F.desc("fecha_carga_ts"), F.desc("_archivo_origen"))))
        .filter("fila = 1").drop("fila")
    )
    n_duplicados = n_recibidas - df.count()

    # --- 4.3 Reglas de calidad: se registra el PRIMER motivo que falla
    df = df.join(lotes_validos, "codigo_lote", "left")
    motivo = (
        F.when(F.col("id_registro").isNull(), "id_nulo")
        .when(F.col("codigo_lote").isNull() | (F.col("codigo_lote") == ""), "lote_vacio")
        .when(F.col("area_ha").isNull(), "lote_inexistente")
        .when(F.col("fecha_cosecha_dt").isNull(), "fecha_invalida")
        .when(F.col("fecha_cosecha_dt") > F.to_date("fecha_carga_ts"), "fecha_futura")
        .when(F.col("kilos_exp").isNull() | (F.col("kilos_exp") < 0), "kilos_negativos_o_nulos")
        .when(F.col("kilos_exp") / F.col("area_ha") > KG_HA_MAX_REGISTRO, "kilos_fuera_de_rango")
    )
    df = df.withColumn("motivo_rechazo", motivo).withColumn("_fecha_proceso", F.current_timestamp())

    validos = df.filter("motivo_rechazo IS NULL").select(
        "id_registro", F.col("fecha_cosecha_dt").alias("fecha_cosecha"), "codigo_lote", "turno",
        F.col("kilos_exp").alias("kilos_exportables"), F.col("kilos_desc").alias("kilos_descarte"),
        F.col("fecha_carga_ts").alias("fecha_carga"), "_archivo_origen", "_fecha_proceso",
    )
    rechazados = df.filter("motivo_rechazo IS NOT NULL").select(
        "id_registro", F.col("fecha_cosecha").alias("fecha_cosecha_original"), "codigo_lote",
        F.col("kilos_exportables").alias("kilos_exportables_original"), "motivo_rechazo",
        "_archivo_origen", "_fecha_proceso",
    )

    # --- 4.4 MERGE de válidos: inserta lo nuevo, actualiza solo si llega una carga más reciente
    (
        DeltaTable.forName(sesion, f"{catalogo}.silver.cosecha").alias("t")
        .merge(validos.alias("s"), "t.id_registro = s.id_registro")
        .whenMatchedUpdateAll(condition="s.fecha_carga > t.fecha_carga")
        .whenNotMatchedInsertAll()
        .execute()
    )

    # --- 4.5 MERGE de rechazados (clave id + archivo: re-ejecutar no duplica la cuarentena)
    (
        DeltaTable.forName(sesion, f"{catalogo}.silver.cosecha_cuarentena").alias("t")
        .merge(rechazados.alias("s"), "t.id_registro <=> s.id_registro AND t._archivo_origen = s._archivo_origen")
        .whenNotMatchedInsertAll()
        .execute()
    )

    # --- 4.6 Log de calidad del lote
    conteo_motivos = {r["motivo_rechazo"]: r["count"] for r in rechazados.groupBy("motivo_rechazo").count().collect()}
    n_validos = validos.count()
    sesion.createDataFrame(
        [("silver.cosecha", batch_id, n_recibidas, n_validos, sum(conteo_motivos.values()), n_duplicados, conteo_motivos)],
        "tabla STRING, batch_id BIGINT, filas_recibidas BIGINT, filas_validas BIGINT, filas_rechazadas BIGINT, "
        "duplicados_en_lote BIGINT, rechazos_por_motivo MAP<STRING, BIGINT>",
    ).withColumn("fecha_proceso", F.current_timestamp()).select(
        "tabla", "batch_id", "fecha_proceso", "filas_recibidas", "filas_validas",
        "filas_rechazadas", "duplicados_en_lote", "rechazos_por_motivo",
    ).write.mode("append").saveAsTable(f"{catalogo}.silver.log_calidad")

    print(f"Lote {batch_id}: {n_recibidas:,} recibidas | {n_validos:,} válidas | "
          f"{sum(conteo_motivos.values()):,} cuarentena {conteo_motivos} | {n_duplicados:,} duplicadas")

# COMMAND ----------

# ==== 5. EJECUCIÓN INCREMENTAL ====
(
    spark.readStream.table(f"{catalogo}.bronze.cosecha")
    .writeStream
    .foreachBatch(procesar_lote)
    .option("checkpointLocation", f"{ruta_checkpoints}/silver_cosecha")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

display(spark.table(f"{catalogo}.silver.log_calidad").orderBy(F.desc("fecha_proceso")).limit(10))
