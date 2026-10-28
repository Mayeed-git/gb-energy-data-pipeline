
{{
    config(
        materialized='table',
        partition_by={
            "field": "observation_date",
            "data_type": "date"
        },
        cluster_by=["location_id"]
    )
}}

WITH source_data AS (

    SELECT *
    FROM {{ ref('int_energy_weather_hourly') }}

),

final AS (

    SELECT
        observation_hour,

        DATE(observation_hour) AS observation_date,

        uk_date,
        location_id,

        avg_carbon_intensity AS carbon_intensity_gco2_kwh,
        avg_forecast_intensity AS forecast_intensity_gco2_kwh,

        avg_wind_pct AS wind_generation_pct,
        avg_solar_pct AS solar_generation_pct,
        avg_gas_pct AS gas_generation_pct,
        avg_nuclear_pct AS nuclear_generation_pct,
        avg_hydro_pct AS hydro_generation_pct,
        avg_biomass_pct AS biomass_generation_pct,

        avg_selected_renewable_pct
            AS selected_renewable_pct,

        avg_total_generation_pct
            AS total_generation_pct,

        temperature_2m AS temperature_c,
        wind_speed_10m AS wind_speed_kmh,
        cloud_cover AS cloud_cover_pct,

        carbon_interval_count,
        generation_interval_count,

        is_complete_hour

    FROM source_data

    WHERE is_complete_hour = TRUE

)

SELECT *
FROM final
