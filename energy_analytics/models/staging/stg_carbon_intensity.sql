
{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        period_start,
        period_end,
        actual_intensity,
        forecast_intensity,
        ingested_at,
        source_file
    FROM {{ source('energy_raw', 'carbon_intensity') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY period_start
            ORDER BY ingested_at DESC, source_file DESC
        ) AS row_num
    FROM source_data
    WHERE period_start IS NOT NULL
)

SELECT
    period_start,
    period_end,
    actual_intensity,
    forecast_intensity,

    DATE(period_start, 'Europe/London') AS uk_date,

    EXTRACT(
        HOUR FROM DATETIME(period_start, 'Europe/London')
    ) AS uk_hour,

    ingested_at,
    source_file

FROM deduplicated
WHERE row_num = 1
