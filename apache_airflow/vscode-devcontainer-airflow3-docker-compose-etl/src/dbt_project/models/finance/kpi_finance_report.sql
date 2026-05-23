{{ config(materialized='table') }}

WITH silver_sales AS (
    SELECT 
        id_venta,
        CAST(fecha AS DATE) AS fecha_venta,
        sucursal_id,
        monto,
        estado_sucursal,
        region
    FROM {{ source('silver_layer', 'ventas_consolidadas') }}
)

SELECT
    region,
    EXTRACT(YEAR FROM fecha_venta) AS anio,
    EXTRACT(MONTH FROM fecha_venta) AS mes,
    COUNT(DISTINCT id_venta) AS transacciones_totales,
    SUM(monto) AS ingresos_totales,
    AVG(monto) AS ticket_promedio,
    -- KPI de riesgo: Alerta si hay transacciones en sucursales con problemas
    SUM(CASE WHEN estado_sucursal IN ('CRITICO', 'CERRADO') THEN monto ELSE 0 END) AS ingresos_en_riesgo
FROM silver_sales
GROUP BY 1, 2, 3
ORDER BY ingresos_totales DESC