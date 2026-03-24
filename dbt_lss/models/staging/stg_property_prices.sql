-- stg_property_prices.sql
-- Staging: Property prices aggregated by ward + year
-- Validation label only — not an ML input feature

WITH ward_mapped AS (
    SELECT
        p.locality,
        p.source,
        p.price_total,
        p.price_sqft,
        p.area_sqft,
        p.bedrooms,
        p.property_type,
        COALESCE(EXTRACT(YEAR FROM p.listing_date), EXTRACT(YEAR FROM p.scraped_at))::INTEGER AS year,
        -- Map to ward via name match or PostGIS
        COALESCE(
            w_name.ward_id,
            w_geo.ward_id
        ) AS ward_id,
        COALESCE(
            w_name.ward_name,
            w_geo.ward_name,
            p.ward_name
        ) AS ward_name
    FROM {{ source('raw', 'property_prices') }} p
    LEFT JOIN {{ source('raw', 'ward_boundaries') }} w_name
        ON LOWER(TRIM(p.ward_name)) = LOWER(TRIM(w_name.ward_name))
    LEFT JOIN {{ source('raw', 'ward_boundaries') }} w_geo
        ON p.ward_name IS NULL  -- Only use geo-join if name match fails
    WHERE p.price_sqft IS NOT NULL
      AND p.price_sqft BETWEEN 1000 AND 100000  -- Sanity filter
),

price_agg AS (
    SELECT
        ward_id,
        ward_name,
        year,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_sqft), 2) AS median_price_sqft,
        ROUND(AVG(price_sqft), 2) AS avg_price_sqft,
        ROUND(MIN(price_sqft), 2) AS min_price_sqft,
        ROUND(MAX(price_sqft), 2) AS max_price_sqft,
        COUNT(*) AS listing_count,
        MODE() WITHIN GROUP (ORDER BY property_type) AS dominant_property_type
    FROM ward_mapped
    WHERE ward_id IS NOT NULL
      AND year BETWEEN 2018 AND 2024
    GROUP BY ward_id, ward_name, year
),

with_yoy AS (
    SELECT
        pa.*,
        CASE
            WHEN LAG(pa.median_price_sqft) OVER (
                PARTITION BY pa.ward_id ORDER BY pa.year
            ) > 0 THEN
                ROUND(
                    (pa.median_price_sqft - LAG(pa.median_price_sqft) OVER (
                        PARTITION BY pa.ward_id ORDER BY pa.year
                    )) / LAG(pa.median_price_sqft) OVER (
                        PARTITION BY pa.ward_id ORDER BY pa.year
                    ), 4
                )
            ELSE NULL
        END AS price_change_yoy
    FROM price_agg pa
)

SELECT
    ward_id,
    ward_name,
    year,
    median_price_sqft,
    avg_price_sqft,
    min_price_sqft,
    max_price_sqft,
    listing_count,
    COALESCE(price_change_yoy, 0)::NUMERIC(8, 4) AS price_change_yoy,
    dominant_property_type,
    NOW() AS updated_at
FROM with_yoy
