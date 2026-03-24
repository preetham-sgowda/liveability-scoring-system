-- stg_ndvi.sql
-- Staging: NDVI aggregated to annual ward-level with YoY change

WITH monthly_ndvi AS (
    SELECT
        ward_id,
        ward_name,
        year,
        ROUND(AVG(mean_ndvi), 4) AS avg_ndvi,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_ndvi), 4) AS median_ndvi,
        ROUND(MIN(min_ndvi), 4) AS min_ndvi,
        ROUND(MAX(max_ndvi), 4) AS max_ndvi,
        SUM(pixel_count) AS total_pixels
    FROM {{ source('raw', 'ndvi') }}
    WHERE ward_id IS NOT NULL
      AND year BETWEEN 2018 AND 2024
    GROUP BY ward_id, ward_name, year
),

with_yoy AS (
    SELECT
        n.*,
        -- Year-over-year NDVI change
        n.avg_ndvi - LAG(n.avg_ndvi) OVER (
            PARTITION BY n.ward_id ORDER BY n.year
        ) AS ndvi_change_yoy,
        -- Green cover estimate: % of pixels with NDVI > 0.3
        -- (approximated from mean; actual requires raster-level analysis)
        CASE
            WHEN n.avg_ndvi > 0.6 THEN 80.0
            WHEN n.avg_ndvi > 0.4 THEN 60.0
            WHEN n.avg_ndvi > 0.3 THEN 40.0
            WHEN n.avg_ndvi > 0.2 THEN 20.0
            ELSE 10.0
        END AS green_cover_pct
    FROM monthly_ndvi n
)

SELECT
    ward_id,
    ward_name,
    year,
    avg_ndvi,
    median_ndvi,
    min_ndvi,
    max_ndvi,
    COALESCE(ndvi_change_yoy, 0)::NUMERIC(6, 4) AS ndvi_change_yoy,
    green_cover_pct::NUMERIC(5, 2) AS green_cover_pct,
    NOW() AS updated_at
FROM with_yoy
