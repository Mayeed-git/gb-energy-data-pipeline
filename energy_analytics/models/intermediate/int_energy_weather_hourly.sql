
{{ config(materialized='view') }}

WITH electricity AS (

    SELECT
        observation_hour,
        uk_date,
        avg_carbon_intensity,
        avg_forecast_intensity,
        avg_wind_pct,
        avg_solar_pct,
        avg_gas_pct,
        avg_nuclear_pct,
        avg_hydro_pct,
        avg_biomass_pct,
        avg_selected_renewable_pct,
        avg_total_generation_pct,
        carbon_interval_count,
        generation_interval_count,
        is_complete_hour

    FROM {{ ref('int_electricity_hourly') }}

),

weather AS (

    SELECT
        observation_time,
        location_id,
        temperature_2m,
        wind_speed_10m,
        cloud_cover

    FROM {{ ref('stg_weather') }}

),

combined AS (

    SELECT
        e.observation_hour,
        e.uk_date,

        w.location_id,

        e.avg_carbon_intensity,
        e.avg_forecast_intensity,

        e.avg_wind_pct,
        e.avg_solar_pct,
        e.avg_gas_pct,
        e.avg_nuclear_pct,
        e.avg_hydro_pct,
        e.avg_biomass_pct,
        e.avg_selected_renewable_pct,
        e.avg_total_generation_pct,

        w.temperature_2m,
        w.wind_speed_10m,
        w.cloud_cover,

        e.carbon_interval_count,
        e.generation_interval_count,
        e.is_complete_hour

    FROM electricity e

    INNER JOIN weather w
        ON e.observation_hour = w.observation_time

)

SELECT *
FROM combined
