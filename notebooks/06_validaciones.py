# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Validaciones de datos (pruebas del pipeline)
# MAGIC Si alguna regla falla, el notebook lanza un error → **el Job queda en rojo** y no se publica un dashboard con datos malos.

# COMMAND ----------

# ==== 1. PARÁMETROS ====
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")
t = lambda nombre: spark.table(f"{catalogo}.{nombre}")

# COMMAND ----------

# ==== 2. REGLAS ====
bronze_ids = t("bronze.cosecha").select("id_registro").distinct()
silver_ids = t("silver.cosecha").select("id_registro")
cuarentena_ids = t("silver.cosecha_cuarentena").select("id_registro").distinct()

chequeos = {
    # --- 2.1 Unicidad y nulos en claves
    "silver.cosecha sin id_registro duplicado":
        t("silver.cosecha").groupBy("id_registro").count().filter("count > 1").count() == 0,
    "silver.cosecha sin claves nulas":
        t("silver.cosecha").filter("id_registro IS NULL OR codigo_lote IS NULL OR fecha_cosecha IS NULL").count() == 0,
    "silver.clima_diario único por estación-fecha":
        t("silver.clima_diario").groupBy("estacion", "fecha").count().filter("count > 1").count() == 0,
    # --- 2.2 Rangos de negocio
    "silver.cosecha sin kilos negativos":
        t("silver.cosecha").filter("kilos_exportables < 0").count() == 0,
    "silver.clima_diario con ≥ 95% de días válidos":
        t("silver.clima_diario").agg(F.avg(F.col("dia_valido").cast("int"))).first()[0] >= 0.95,
    # --- 2.3 Reconciliación: todo id de bronze terminó en silver o en cuarentena (no se pierden registros)
    "bronze = silver ∪ cuarentena (sin pérdidas)":
        bronze_ids.join(silver_ids.union(cuarentena_ids).distinct(), "id_registro", "left_anti").count() == 0,
    # --- 2.4 Gold completo
    "gold.cosecha_clima_diaria con clima en ≥ 99% de filas":
        t("gold.cosecha_clima_diaria").agg(F.avg(F.col("temp_media").isNotNull().cast("int"))).first()[0] >= 0.99,
}

# COMMAND ----------

# ==== 3. RESULTADO ====
for regla, ok in chequeos.items():
    print(f"{'✅' if ok else '❌'} {regla}")

fallidas = [regla for regla, ok in chequeos.items() if not ok]
if fallidas:
    raise AssertionError(f"{len(fallidas)} validación(es) fallaron: {fallidas}")
print("\n✅ Todas las validaciones pasaron")
