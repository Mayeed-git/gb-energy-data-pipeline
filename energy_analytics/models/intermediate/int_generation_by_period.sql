
{{ config(materialized='view') }}

WITH generation AS (
    SELECT
        period_start,
        period_end,
        fuel_type,
        generation_pct
    FROM {{ ref('stg_generation_mix') }}
),

aggregated AS (
    SELECT
        period_start,
        MAX(period_end) AS period_end,

        SUM(IF(fuel_type = 'wind', generation_pct, 0))
            AS wind_pct,

        SUM(IF(fuel_type = 'solar', generation_pct, 0))
            AS solar_pct,

        SUM(IF(fuel_type = 'gas', generation_pct, 0))
            AS gas_pct,

        SUM(IF(fuel_type = 'nuclear', generation_pct, 0))
            AS nuclear_pct,

        SUM(IF(fuel_type = 'hydro', generation_pct, 0))
            AS hydro_pct,

        SUM(IF(fuel_type = 'biomass', generation_pct, 0))
            AS biomass_pct,

        SUM(generation_pct) AS total_generation_pct,
        COUNT(DISTINCT fuel_type) AS fuel_type_count

    FROM generation
    GROUP BY period_start
)

SELECT
    *,
    wind_pct + solar_pct + hydro_pct AS selected_renewable_pct
FROM aggregated
