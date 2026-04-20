-- Sensors sometimes malfunction and send negative voltage readings.

-- This test identifies those erroneous readings.
SELECT
    reading_id,
    voltage
FROM {{ ref('stg_readings') }}
WHERE voltage <= 0