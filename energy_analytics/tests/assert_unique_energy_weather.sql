SELECT
    observation_hour,
    location_id,
    COUNT(*) AS duplicate_count
FROM {{ ref('fct_energy_weather') }}
GROUP BY observation_hour, location_id
HAVING COUNT(*) > 1
