# %%
# =============================================================================
# DESCARGA DE CLIMA HORARIO HISTÓRICO — OPEN-METEO (correr en tu PC, una sola vez)
# =============================================================================
# Por qué se corre local: Databricks Free Edition limita la salida a internet a dominios de
# confianza. Este script baja el clima, guarda un CSV por estación y luego se sube al Volume.
#
# Fuente: Open-Meteo Historical Weather API (https://open-meteo.com) — licencia CC BY 4.0.
# Uso:  pip install requests pandas   →   python scripts/descargar_clima_open_meteo.py

# ==== 1. PARÁMETROS ====
from pathlib import Path
import time

import pandas as pd
import requests

# Valles agroexportadores del norte del Perú (coordenadas públicas aproximadas de cada localidad)
ESTACIONES = {
    'chao':  {'latitude': -8.54, 'longitude': -78.68},
    'viru':  {'latitude': -8.41, 'longitude': -78.75},
}

FECHA_INICIO = '2025-07-01'   # 1 mes antes de la campaña simulada (para ventanas móviles)

FECHA_FIN = '2026-01-31'

VARIABLES = [
    'temperature_2m', 'relative_humidity_2m', 'dew_point_2m', 'precipitation',
    'vapour_pressure_deficit', 'shortwave_radiation', 'wind_speed_10m', 'et0_fao_evapotranspiration',
]

CARPETA_SALIDA = Path(__file__).resolve().parents[1] / 'data' / 'clima'

CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)

# %%
# ==== 2. DESCARGA POR ESTACIÓN (con reintentos) ====
for estacion, coord in ESTACIONES.items():
    params = {
        **coord, 'start_date': FECHA_INICIO, 'end_date': FECHA_FIN,
        'hourly': ','.join(VARIABLES), 'timezone': 'America/Lima',
    }
    for intento in range(1, 4):
        try:
            respuesta = requests.get('https://archive-api.open-meteo.com/v1/archive', params=params, timeout=60)
            respuesta.raise_for_status()
            break
        except requests.RequestException as e:
            print(f"⚠️ {estacion}: intento {intento} falló → {e}")
            if intento == 3:
                raise
            time.sleep(5 * intento)

    print("URL:", respuesta.url)
    print("STATUS:", respuesta.status_code)
    print("CONTENT-TYPE:", respuesta.headers.get("Content-Type"))
    print("RESPUESTA:", respuesta.text[:1000])

    df = pd.DataFrame(respuesta.json()['hourly']).rename(columns={'time': 'fecha_hora'})
    df.insert(0, 'estacion', estacion)

    ruta = CARPETA_SALIDA / f'clima_horario_{estacion}.csv'
    df.to_csv(ruta, index=False)
    print(f"✅ {estacion}: {len(df):,} horas | {df['fecha_hora'].min()} a {df['fecha_hora'].max()} → {ruta.name}")