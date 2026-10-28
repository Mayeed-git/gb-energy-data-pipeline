
{{ config(materialized='view') }}

WITH carbon AS (
    SELECT
        period_start,
        period_end,
        actual_intensity,
        forecast_intensity
    FROM {{ ref('stg_carbon_intensity') }}
),

generation AS (
    SELECT
        period_start,
        wind_pct,
        solar_pct,
        gas_pct,
        nuclear_pct,
        hydro_pct,
        biomass_pct,
        selected_renewable_pct,
        total_generation_pct
    FROM {{ ref('int_generation_by_period') }}
),

combined AS (
    SELECT
        c.period_start,
        c.actual_intensity,
        c.forecast_intensity,
        g.wind_pct,
        g.solar_pct,
        g.gas_pct,
        g.nuclear_pct,
        g.hydro_pct,
        g.biomass_pct,
        g.selected_renewable_pct,
        g.total_generation_pct,
        g.period_start IS NOT NULL AS has_generation_data
    FROM carbon c
    LEFT JOIN generation g
        ON c.period_start = g.period_start
),

hourly AS (
    SELECT
        TIMESTAMP_TRUNC(period_start, HOUR, 'UTC')
            AS observation_hour,

        COUNT(*) AS carbon_interval_count,

        COUNTIF(has_generation_data)
            AS generation_interval_count,

        ROUND(AVG(actual_intensity), 2)
            AS avg_carbon_intensity,

        ROUND(AVG(forecast_intensity), 2)
            AS avg_forecast_intensity,

        ROUND(AVG(wind_pct), 2) AS avg_wind_pct,
        ROUND(AVG(solar_pct), 2) AS avg_solar_pct,
        ROUND(AVG(gas_pct), 2) AS avg_gas_pct,
        ROUND(AVG(nuclear_pct), 2) AS avg_nuclear_pct,
        ROUND(AVG(hydro_pct), 2) AS avg_hydro_pct,
        ROUND(AVG(biomass_pct), 2) AS avg_biomass_pct,

        ROUND(AVG(selected_renewable_pct), 2)
            AS avg_selected_renewable_pct,

        ROUND(AVG(total_generation_pct), 2)
            AS avg_total_generation_pct

    FROM combined
    GROUP BY 1
)

SELECT
    *,
    DATE(observation_hour, 'Europe/London') AS uk_date,

    carbon_interval_count = 2
      AND generation_interval_count = 2
      AS is_complete_hour

FROM hourly
