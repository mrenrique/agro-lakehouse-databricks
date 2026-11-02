# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup del lakehouse
# MAGIC Crea los esquemas de la arquitectura medallion y los Volumes donde aterrizan los archivos.
# MAGIC Es idempotente: se puede correr las veces que sea sin duplicar nada (`IF NOT EXISTS`).
# MAGIC
# MAGIC | Esquema | Contenido |
# MAGIC |---|---|
# MAGIC | `landing` | Volumes con archivos crudos (`archivos`) y checkpoints de streaming (`checkpoints`) |
# MAGIC | `bronze` | Copia fiel de los archivos, todo como texto + metadatos de origen |
# MAGIC | `silver` | Datos tipados, validados y deduplicados; cuarentena de rechazados; log de calidad |
# MAGIC | `gold` | Tablas analíticas listas para dashboard y modelos |

# COMMAND ----------

# ==== 1. PARÁMETROS ====
# En Free Edition el catálogo por defecto se llama "workspace"
dbutils.widgets.text("catalogo", "workspace")
catalogo = dbutils.widgets.get("catalogo")

# COMMAND ----------

# ==== 2. ESQUEMAS Y VOLUMES ====
for esquema in ["landing", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{esquema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.landing.archivos")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.landing.checkpoints")

# Subcarpetas por fuente (cada una la lee un Auto Loader distinto)
for carpeta in ["cosecha", "clima", "maestro"]:
    dbutils.fs.mkdirs(f"/Volumes/{catalogo}/landing/archivos/{carpeta}")

display(spark.sql(f"SHOW SCHEMAS IN {catalogo}"))
print(f"✅ Listo. Sube los CSV de clima a /Volumes/{catalogo}/landing/archivos/clima/")
