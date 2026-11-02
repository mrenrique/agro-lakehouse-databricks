# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Simulador de archivos de cosecha
# MAGIC Genera archivos CSV diarios con el formato de una exportación de cosecha (tipo SAP), **con errores
# MAGIC sembrados a propósito** para que el pipeline tenga algo real que validar:
# MAGIC
# MAGIC | Error simulado | % aprox. | Qué debe hacer el pipeline |
# MAGIC |---|---|---|
# MAGIC | Fecha en formato `dd/MM/yyyy` | 3% | Parsearla (no es error de negocio) |
# MAGIC | Registro reenviado al día siguiente (a veces con kilos corregidos) | 2% | Deduplicar y quedarse con la última versión (`MERGE`) |
# MAGIC | Kilos negativos | 0.5% | Cuarentena |
# MAGIC | Lote vacío / lote inexistente | 0.4% c/u | Cuarentena |
# MAGIC | Fecha de cosecha futura | 0.3% | Cuarentena |
# MAGIC | Kilos ×20 (error de digitación) | 0.3% | Cuarentena por rango (kg/ha) |
# MAGIC
# MAGIC **Los datos de cosecha son sintéticos** (no pertenecen a ninguna empresa). El clima sí es real (Open-Meteo).
# MAGIC Es determinista: la misma fecha genera siempre el mismo archivo, así que re-ejecutar no cambia nada.

# COMMAND ----------

# ==== 1. PARÁMETROS ====
import os
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.dropdown("modo", "historico", ["historico", "siguiente_dia"])
dbutils.widgets.text("fecha_inicio", "2025-08-01")
dbutils.widgets.text("fecha_fin_historico", "2025-11-30")

catalogo = dbutils.widgets.get("catalogo")
modo = dbutils.widgets.get("modo")
fecha_inicio = date.fromisoformat(dbutils.widgets.get("fecha_inicio"))
fecha_fin_historico = date.fromisoformat(dbutils.widgets.get("fecha_fin_historico"))
FECHA_MAXIMA = date(2026, 1, 31)   # hasta donde hay clima descargado

ruta_cosecha = f"/Volumes/{catalogo}/landing/archivos/cosecha"
ruta_maestro = f"/Volumes/{catalogo}/landing/archivos/maestro"

# COMMAND ----------

# ==== 2. MAESTRO DE LOTES (fijo, semilla 42) ====
# 3 valles × 20 lotes. Cada valle tiene su estación de clima.
rng_maestro = np.random.default_rng(42)
valles = {"olmos": date(2025, 9, 5), "chao": date(2025, 10, 10), "viru": date(2025, 10, 20)}   # pico de campaña
desfase_variedad = {"Biloxi": -10, "Ventura": 5, "Emerald": 15}                              # días vs pico del valle

filas_maestro = []
for valle, fecha_pico_valle in valles.items():
    for i in range(20):
        variedad = list(desfase_variedad)[i % 3]
        filas_maestro.append({
            "codigo_lote": f"{valle[:2].upper()}{i + 1:03d}",
            "valle": valle,
            "estacion_clima": valle,
            "variedad": variedad,
            "area_ha": round(float(rng_maestro.uniform(2.0, 6.0)), 2),
            "fecha_pico": fecha_pico_valle + timedelta(days=desfase_variedad[variedad] + int(rng_maestro.integers(-7, 8))),
            "desv_dias": float(rng_maestro.uniform(22, 30)),
            "pico_kg_ha_dia": float(rng_maestro.uniform(180, 280)),
            "frecuencia_dias": int(rng_maestro.choice([3, 4])),
            "desfase_pasada": int(rng_maestro.integers(0, 4)),
        })
maestro = pd.DataFrame(filas_maestro)

# Solo se escribe una vez: el Auto Loader no vuelve a leer un archivo ya ingerido
ruta_archivo_maestro = f"{ruta_maestro}/maestro_lotes.csv"
if not os.path.exists(ruta_archivo_maestro):
    maestro[["codigo_lote", "valle", "estacion_clima", "variedad", "area_ha"]].to_csv(ruta_archivo_maestro, index=False)
    print(f"✅ Maestro escrito: {len(maestro)} lotes")
else:
    print("ℹ️ Maestro ya existe, no se reescribe")

# COMMAND ----------

# ==== 3. GENERADOR DE UN DÍA ====
# Se usa para el día que se escribe y para el día anterior (de ahí salen los reenvíos)
COLUMNAS = ["id_registro", "fecha_cosecha", "codigo_lote", "turno", "kilos_exportables", "kilos_descarte", "fecha_carga"]

def generar_registros_dia(fecha):
    rng = np.random.default_rng(int(fecha.strftime("%Y%m%d")))
    registros = []
    for lote in maestro.itertuples():
        dia_indice = (fecha - fecha_inicio).days
        if (dia_indice + lote.desfase_pasada) % lote.frecuencia_dias != 0:
            continue   # hoy no le toca pasada a este lote

        # Producción acumulada desde la pasada anterior (curva gaussiana de campaña)
        dias = np.array([(fecha - timedelta(days=k) - lote.fecha_pico).days for k in range(lote.frecuencia_dias)])
        kg_ha = lote.pico_kg_ha_dia * np.exp(-0.5 * (dias / lote.desv_dias) ** 2).sum()
        kilos = kg_ha * lote.area_ha * rng.lognormal(0, 0.12)
        if kilos < 5:
            continue

        # A veces la pasada se reparte en dos turnos → dos registros del mismo lote y día
        turnos = {"M": 0.6, "T": 0.4} if rng.random() < 0.3 else {"M": 1.0}
        for turno, proporcion in turnos.items():
            kilos_turno = kilos * proporcion
            registros.append({
                "id_registro": f"{fecha:%Y%m%d}-{lote.codigo_lote}-{turno}",
                "fecha_cosecha": fecha.isoformat(),
                "codigo_lote": lote.codigo_lote,
                "turno": turno,
                "kilos_exportables": round(kilos_turno, 1),
                "kilos_descarte": round(kilos_turno * rng.uniform(0.06, 0.15), 1),
                "fecha_carga": (datetime.combine(fecha + timedelta(days=1), datetime.min.time())
                                + timedelta(hours=6, minutes=int(rng.integers(0, 120)))).isoformat(sep=" "),
            })
    return pd.DataFrame(registros, columns=COLUMNAS), rng

# COMMAND ----------

# ==== 4. ARCHIVO DEL DÍA (con errores y reenvíos) ====
def escribir_archivo_dia(fecha):
    df_dia, rng = generar_registros_dia(fecha)
    n = len(df_dia)

    # --- 4.1 Errores sembrados (cada fila recibe como máximo uno)
    sorteo = rng.random(n)
    df_dia.loc[sorteo < 0.03, "fecha_cosecha"] = pd.to_datetime(df_dia.loc[sorteo < 0.03, "fecha_cosecha"]).dt.strftime("%d/%m/%Y")
    df_dia.loc[(sorteo >= 0.03) & (sorteo < 0.035), "kilos_exportables"] *= -1
    df_dia.loc[(sorteo >= 0.035) & (sorteo < 0.039), "codigo_lote"] = ""
    df_dia.loc[(sorteo >= 0.039) & (sorteo < 0.043), "codigo_lote"] = "XX999"
    df_dia.loc[(sorteo >= 0.043) & (sorteo < 0.046), "fecha_cosecha"] = (fecha + timedelta(days=40)).isoformat()
    df_dia.loc[(sorteo >= 0.046) & (sorteo < 0.049), "kilos_exportables"] *= 20

    # --- 4.2 Reenvíos del día anterior: misma clave, carga más reciente, a veces kilos corregidos
    df_ayer, _ = generar_registros_dia(fecha - timedelta(days=1))
    if len(df_ayer) > 0 and fecha > fecha_inicio:
        reenvios = df_ayer.sample(frac=0.02, random_state=int(fecha.strftime("%Y%m%d"))).copy()
        corregidos = rng.random(len(reenvios)) < 0.5
        reenvios.loc[corregidos, "kilos_exportables"] = (reenvios.loc[corregidos, "kilos_exportables"] * rng.uniform(0.9, 1.1)).round(1)
        reenvios["fecha_carga"] = df_dia["fecha_carga"].max() if n else reenvios["fecha_carga"]
        df_dia = pd.concat([df_dia, reenvios], ignore_index=True)

    df_dia.to_csv(f"{ruta_cosecha}/cosecha_{fecha:%Y%m%d}.csv", index=False)
    return len(df_dia)

# COMMAND ----------

# ==== 5. EJECUCIÓN SEGÚN MODO ====
# historico     → escribe todos los días entre fecha_inicio y fecha_fin_historico (primera carga)
# siguiente_dia → escribe solo el día siguiente al último archivo existente (simula la llegada diaria)
archivos_existentes = sorted(f for f in os.listdir(ruta_cosecha) if f.startswith("cosecha_"))

if modo == "historico":
    fechas = [fecha_inicio + timedelta(days=d) for d in range((fecha_fin_historico - fecha_inicio).days + 1)]
else:
    if not archivos_existentes:
        raise ValueError("No hay archivos previos: corre primero en modo 'historico'.")
    ultima_fecha = datetime.strptime(archivos_existentes[-1][8:16], "%Y%m%d").date()
    fechas = [ultima_fecha + timedelta(days=1)]

fechas = [f for f in fechas if f <= FECHA_MAXIMA]
if not fechas:
    dbutils.notebook.exit("Se llegó al fin del rango con clima disponible. Nada que generar.")

total = sum(escribir_archivo_dia(f) for f in fechas)
print(f"✅ {len(fechas)} archivo(s) | {total:,} registros | {fechas[0]} → {fechas[-1]}")
