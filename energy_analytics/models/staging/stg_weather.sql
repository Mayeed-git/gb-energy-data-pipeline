
{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        observation_time,
        LOWER(TRIM(location_id)) AS location_id,
        temperature_2m,
        wind_speed_10m,
        cloud_cover,
        ingested_at,
        source_file,

        ROW_NUMBER() OVER (
            PARTITION BY observation_time, LOWER(TRIM(location_id))
            ORDER BY ingested_at DESC, source_file DESC
        ) AS row_num

    FROM {{ source('energy_raw', 'weather') }}

    WHERE observation_time IS NOT NULL
      AND location_id IS NOT NULL
)

SELECT
    observation_time,
    location_id,
    temperature_2m,
    wind_speed_10m,
    cloud_cover,
    DATE(observation_time, 'Europe/London') AS uk_date,
    ingested_at,
    source_file

FROM ranked
WHERE row_num = 1
