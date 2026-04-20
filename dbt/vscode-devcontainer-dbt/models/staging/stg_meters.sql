{{ config(materialized='view') }}

-- Import of the raw source defined in sources.yml
WITH source AS (
    SELECT * FROM {{ source('excel_files', 'meters') }}
),

-- Cleaning and standardization of names
renamed AS (
    SELECT
        -- We convert to string and remove any spaces.
        TRIM(CAST(id_meter AS VARCHAR)) as meter_id,
        
        -- We standardized column names for the business team
        CAST(location AS VARCHAR) as location_name,
        
        -- Simple categorization (e.g., Residential, Industrial)
        UPPER(CAST(customer_type AS VARCHAR)) as customer_category,
        
        -- Basic audit: when this record was loaded into the Raw layer
        CAST(installation_date AS TIMESTAMP) as installation_date
    FROM source
)

SELECT * FROM renamed