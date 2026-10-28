
{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        period_start,
        period_end,
        LOWER(TRIM(fuel_type)) AS fuel_type,
        generation_pct,
        ingested_at,
        source_file,

        ROW_NUMBER() OVER (
            PARTITION BY period_start, LOWER(TRIM(fuel_type))
            ORDER BY ingested_at DESC, source_file DESC
        ) AS row_num

    FROM {{ source('energy_raw', 'generation_mix') }}

    WHERE period_start IS NOT NULL
      AND fuel_type IS NOT NULL
)

SELECT
    period_start,
    period_end,
    fuel_type,
    generation_pct,
    DATE(period_start, 'Europe/London') AS uk_date,
    ingested_at,
    source_file

FROM ranked
WHERE row_num = 1
