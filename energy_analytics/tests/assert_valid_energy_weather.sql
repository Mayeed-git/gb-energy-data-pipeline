
SELECT *
FROM {{ ref('fct_energy_weather') }}
WHERE carbon_intensity_gco2_kwh < 0
   OR wind_generation_pct NOT BETWEEN 0 AND 100
   OR gas_generation_pct NOT BETWEEN 0 AND 100
   OR solar_generation_pct NOT BETWEEN 0 AND 100
   OR wind_speed_kmh < 0
   OR cloud_cover_pct NOT BETWEEN 0 AND 100
   OR ABS(total_generation_pct - 100) > 1
   OR carbon_interval_count != 2
   OR generation_interval_count != 2
