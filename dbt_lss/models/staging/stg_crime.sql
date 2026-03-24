-- stg_crime.sql
-- Staging: NCRB crime data aggregated by ward + year
-- Since NCRB data is city-level, we distribute uniformly across wards
-- (In production, use FIR station mapping for ward-level granularity)

WITH crime_by_year AS (
    SELECT
        city,
        year,
        SUM(CASE WHEN offense_category = 'murder' THEN count ELSE 0 END) AS murder_count,
        SUM(CASE WHEN offense_category = 'theft' THEN count ELSE 0 END) AS theft_count,
        SUM(CASE WHEN offense_category = 'robbery' THEN count ELSE 0 END) AS robbery_count,
        SUM(CASE WHEN offense_category = 'assault' THEN count ELSE 0 END) AS assault_count,
        SUM(CASE WHEN offense_category = 'kidnapping' THEN count ELSE 0 END) AS kidnapping_count,
        SUM(CASE WHEN offense_category = 'burglary' THEN count ELSE 0 END) AS burglary_count,
        SUM(count) AS total_ipc_offenses
    FROM {{ source('raw', 'ncrb_crime') }}
    WHERE city = 'Bengaluru'
      AND year BETWEEN 2018 AND 2024
    GROUP BY city, year
),

wards AS (
    SELECT
        ward_id,
        ward_name
    FROM {{ source('raw', 'ward_boundaries') }}
),

census_pop AS (
    SELECT
        ward_name,
        year,
        population
    FROM {{ source('raw', 'census') }}
)

SELECT
    w.ward_id,
    w.ward_name,
    c.year,
    -- Distribute city-level counts proportionally by ward population
    COALESCE(
        ROUND(c.total_ipc_offenses * (cp.population::NUMERIC / NULLIF(SUM(cp.population) OVER (PARTITION BY c.year), 0))),
        0
    )::INTEGER AS total_ipc_offenses,
    COALESCE(ROUND(c.murder_count * (cp.population::NUMERIC / NULLIF(SUM(cp.population) OVER (PARTITION BY c.year), 0))), 0)::INTEGER AS murder_count,
    COALESCE(ROUND(c.theft_count * (cp.population::NUMERIC / NULLIF(SUM(cp.population) OVER (PARTITION BY c.year), 0))), 0)::INTEGER AS theft_count,
    COALESCE(ROUND(c.robbery_count * (cp.population::NUMERIC / NULLIF(SUM(cp.population) OVER (PARTITION BY c.year), 0))), 0)::INTEGER AS robbery_count,
    COALESCE(ROUND(c.assault_count * (cp.population::NUMERIC / NULLIF(SUM(cp.population) OVER (PARTITION BY c.year), 0))), 0)::INTEGER AS assault_count,
    COALESCE(ROUND(c.kidnapping_count * (cp.population::NUMERIC / NULLIF(SUM(cp.population) OVER (PARTITION BY c.year), 0))), 0)::INTEGER AS kidnapping_count,
    COALESCE(ROUND(c.burglary_count * (cp.population::NUMERIC / NULLIF(SUM(cp.population) OVER (PARTITION BY c.year), 0))), 0)::INTEGER AS burglary_count,
    -- Crime rate per 1000 population
    CASE
        WHEN cp.population > 0 THEN
            ROUND(
                (c.total_ipc_offenses * (cp.population::NUMERIC / NULLIF(SUM(cp.population) OVER (PARTITION BY c.year), 0)))
                / cp.population * 1000, 4
            )
        ELSE 0
    END AS crime_rate_per_1000,
    c.city || '_ncrb' AS source_pdf,
    NOW() AS updated_at
FROM wards w
CROSS JOIN crime_by_year c
LEFT JOIN census_pop cp
    ON LOWER(TRIM(w.ward_name)) = LOWER(TRIM(cp.ward_name))
    AND c.year = cp.year
