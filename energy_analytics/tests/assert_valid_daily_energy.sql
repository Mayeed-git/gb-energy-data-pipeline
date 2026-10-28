SELECT *
FROM {{ ref('fct_daily_energy') }}
WHERE complete_hours <= 0
   OR complete_hours > total_hours
   OR avg_carbon_intensity < 0
   OR avg_wind_pct NOT BETWEEN 0 AND 100
   OR avg_gas_pct NOT BETWEEN 0 AND 100
   OR avg_selected_renewable_pct NOT BETWEEN 0 AND 100
