
{{
    config(
        materialized='table',
        partition_by={
            "field": "uk_date",
            "data_type": "date"
        }
    )
}}

WITH hourly AS (

    SELECT
        observation_hour,
        uk_date,
        avg_carbon_intensity,
        avg_forecast_intensity,
        avg_wind_pct,
        avg_solar_pct,
        avg_gas_pct,
        avg_nuclear_pct,
        avg_selected_renewable_pct,
        is_complete_hour

    FROM {{ ref('int_electricity_hourly') }}

),

daily AS (

    SELECT
        uk_date,

        COUNT(*) AS total_hours,

        COUNTIF(is_complete_hour)
            AS complete_hours,

        ROUND(AVG(
            IF(is_complete_hour, avg_carbon_intensity, NULL)
        ), 2) AS avg_carbon_intensity,

        ROUND(MAX(
            IF(is_complete_hour, avg_carbon_intensity, NULL)
        ), 2) AS max_carbon_intensity,

        ROUND(MIN(
            IF(is_complete_hour, avg_carbon_intensity, NULL)
        ), 2) AS min_carbon_intensity,

        ROUND(AVG(
            IF(is_complete_hour, avg_wind_pct, NULL)
        ), 2) AS avg_wind_pct,

        ROUND(AVG(
            IF(is_complete_hour, avg_solar_pct, NULL)
        ), 2) AS avg_solar_pct,

        ROUND(AVG(
            IF(is_complete_hour, avg_gas_pct, NULL)
        ), 2) AS avg_gas_pct,

        ROUND(AVG(
            IF(is_complete_hour, avg_nuclear_pct, NULL)
        ), 2) AS avg_nuclear_pct,

        ROUND(AVG(
            IF(is_complete_hour, avg_selected_renewable_pct, NULL)
        ), 2) AS avg_selected_renewable_pct

    FROM hourly
    GROUP BY uk_date

)

SELECT *
FROM daily
WHERE complete_hours > 0
