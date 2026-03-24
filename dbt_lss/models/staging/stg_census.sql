-- stg_census.sql
-- Staging: Census data matched to ward boundaries

WITH census_clean AS (
    SELECT
        ward_name,
        ward_number,
        year,
        population,
        CASE
            WHEN area_sqkm > 0 THEN ROUND(population::NUMERIC / area_sqkm, 2)
            ELSE density
        END AS population_density,
        literacy_rate,
        male_literacy,
        female_literacy,
        household_size AS avg_household_size,
        total_households,
        CASE
            WHEN male_population > 0 THEN
                ROUND(female_population::NUMERIC / male_population * 1000, 2)
            ELSE NULL
        END AS sex_ratio
    FROM {{ source('raw', 'census') }}
    WHERE population IS NOT NULL
      AND population > 0
),

wards AS (
    SELECT ward_id, ward_name, ward_number
    FROM {{ source('raw', 'ward_boundaries') }}
)

SELECT
    w.ward_id,
    w.ward_name,
    c.year,
    c.population,
    c.population_density,
    c.literacy_rate,
    c.male_literacy,
    c.female_literacy,
    c.avg_household_size,
    c.total_households,
    c.sex_ratio,
    NOW() AS updated_at
FROM census_clean c
INNER JOIN wards w
    ON (
        w.ward_number = c.ward_number
        OR LOWER(TRIM(w.ward_name)) = LOWER(TRIM(c.ward_name))
    )
