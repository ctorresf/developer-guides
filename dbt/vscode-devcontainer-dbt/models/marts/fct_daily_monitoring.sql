{{ config(materialized='table') }}

WITH readings AS (
    SELECT * FROM {{ ref('stg_readings') }}
),

meters AS (
    SELECT * FROM {{ ref('stg_meters') }}
),

final AS (
    SELECT
        m.location_name,
        l.meter_id,
        date_trunc('day', l.registration_date) as day,
        AVG(l.voltage) as voltage_average,
        SUM(l.consumption) as consumption_total,
        -- We detected if there was a risk (voltage < 200)
        MAX(CASE WHEN l.voltage < 200 THEN 1 ELSE 0 END) as there_was_a_voltage_drop
    FROM readings l
    JOIN meters m ON l.meter_id = m.meter_id
    GROUP BY 1, 2, 3
)

SELECT * FROM final