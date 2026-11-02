-- =============================================================================
-- CONSULTAS DEL DASHBOARD (Databricks AI/BI Dashboard)
-- Cambiar "workspace" si se usó otro catálogo.
-- =============================================================================

-- ==== 1. Kilos exportables por semana y valle (gráfico de líneas) ====
SELECT semana_inicio, valle, SUM(kilos_exportables) / 1000 AS toneladas
FROM workspace.gold.kpi_semanal
GROUP BY semana_inicio, valle
ORDER BY semana_inicio;

-- ==== 2. Avance acumulado de campaña por variedad (kg/ha acumulado) ====
SELECT semana_inicio, variedad,
       SUM(SUM(kilos_exportables)) OVER (PARTITION BY variedad ORDER BY semana_inicio)
         / MAX(SUM(area_total_ha)) OVER (PARTITION BY variedad) AS kg_ha_acumulado
FROM workspace.gold.kpi_semanal
GROUP BY semana_inicio, variedad
ORDER BY semana_inicio;

-- ==== 3. Rendimiento vs calor de la última semana (dispersión) ====
SELECT codigo_lote, fecha, valle, variedad, kg_ha, temp_media_7d, horas_temp_mayor_30_7d
FROM workspace.gold.cosecha_clima_diaria
WHERE dias_desde_pasada_anterior IS NOT NULL;

-- ==== 4. Calidad de datos: rechazos por motivo y por corrida ====
SELECT fecha_proceso, motivo, cantidad
FROM workspace.silver.log_calidad
LATERAL VIEW explode(rechazos_por_motivo) AS motivo, cantidad
ORDER BY fecha_proceso DESC;

-- ==== 5. Calidad de datos: % de registros válidos por corrida ====
SELECT fecha_proceso, filas_recibidas, filas_validas,
       ROUND(100 * filas_validas / filas_recibidas, 2) AS pct_validos,
       duplicados_en_lote
FROM workspace.silver.log_calidad
ORDER BY fecha_proceso DESC;
