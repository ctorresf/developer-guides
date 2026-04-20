{{ config(materialized='view') }}

SELECT
    reading_id::VARCHAR as reading_id,
    meter_id::VARCHAR as meter_id,
    voltage::float as voltage,
    kwh_consumption::float as consumption,
    recorded_at::timestamp as registration_date
FROM {{ source('excel_files', 'readings') }}