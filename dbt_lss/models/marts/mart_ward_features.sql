-- mart_ward_features.sql
-- Final wide feature table: one row per ward per year (198 wards × 7 years)
-- This table is consumed by Preetham's ML layer (KMeans, XGBoost, SHAP)

-- Materialized as a table in the marts schema
{{ config(materialized='table', schema='marts') }}

WITH wards AS (
    SELECT
        wb.ward_id,
        wb.ward_name,
        wb.ward_number,
        wb.zone_name,
        wb.geom
    FROM {{ source('raw', 'ward_boundaries') }} wb
),

years AS (
    SELECT generate_series(2018, 2024) AS year
),

ward_years AS (
    SELECT w.ward_id, w.ward_name, w.ward_number, w.zone_name, w.geom, y.year
    FROM wards w
    CROSS JOIN years y
),

crime AS (
    SELECT ward_id, year, total_ipc_offenses, murder_count, theft_count,
           robbery_count, assault_count, kidnapping_count, burglary_count,
           crime_rate_per_1000
    FROM {{ ref('stg_crime') }}
),

complaints AS (
    SELECT ward_id, year, total_complaints, pending_complaints,
           resolved_complaints, resolution_rate, avg_resolution_days
    FROM {{ ref('stg_complaints') }}
),

aqi AS (
    SELECT ward_id, year, avg_pm25, avg_pm10, avg_no2, avg_so2,
           avg_aqi, aqi_good_days_pct
    FROM {{ ref('stg_aqi') }}
),

census AS (
    SELECT ward_id, year, population, population_density, literacy_rate,
           avg_household_size, total_households
    FROM {{ ref('stg_census') }}
),

transit AS (
    SELECT ward_id, year, bus_stops_count, metro_proximity_km,
           avg_route_frequency, total_routes
    FROM {{ ref('stg_transit') }}
),

ndvi AS (
    SELECT ward_id, year, avg_ndvi, ndvi_change_yoy, green_cover_pct
    FROM {{ ref('stg_ndvi') }}
),

property AS (
    SELECT ward_id, year, median_price_sqft, price_change_yoy
    FROM {{ ref('stg_property_prices') }}
)

SELECT
    -- ── Keys ──
    wy.ward_id,
    wy.ward_name,
    wy.ward_number,
    wy.zone_name,
    wy.year,

    -- ── Crime Features ──
    COALESCE(cr.total_ipc_offenses, 0) AS total_ipc_offenses,
    COALESCE(cr.murder_count, 0) AS murder_count,
    COALESCE(cr.theft_count, 0) AS theft_count,
    COALESCE(cr.robbery_count, 0) AS robbery_count,
    COALESCE(cr.assault_count, 0) AS assault_count,
    COALESCE(cr.kidnapping_count, 0) AS kidnapping_count,
    COALESCE(cr.burglary_count, 0) AS burglary_count,
    cr.crime_rate_per_1000,

    -- ── Civic Complaints Features ──
    COALESCE(co.total_complaints, 0) AS total_complaints,
    COALESCE(co.pending_complaints, 0) AS pending_complaints,
    COALESCE(co.resolved_complaints, 0) AS resolved_complaints,
    co.resolution_rate,
    co.avg_resolution_days,

    -- ── Air Quality Features ──
    aq.avg_pm25,
    aq.avg_pm10,
    aq.avg_no2,
    aq.avg_so2,
    aq.avg_aqi,
    aq.aqi_good_days_pct,

    -- ── Census / Demographics Features ──
    ce.population,
    ce.population_density,
    ce.literacy_rate,
    ce.avg_household_size,
    ce.total_households,

    -- ── Transit Features ──
    COALESCE(tr.bus_stops_count, 0) AS bus_stops_count,
    COALESCE(tr.metro_proximity_km, 99.0) AS metro_proximity_km,
    tr.avg_route_frequency,
    COALESCE(tr.total_routes, 0) AS total_routes,

    -- ── Green Cover Features ──
    nd.avg_ndvi,
    nd.ndvi_change_yoy,
    nd.green_cover_pct,

    -- ── Property Price (Validation Label) ──
    pr.median_price_sqft,
    pr.price_change_yoy,

    -- ── Geometry ──
    wy.geom,

    -- ── Metadata ──
    NOW() AS updated_at

FROM ward_years wy
LEFT JOIN crime cr ON wy.ward_id = cr.ward_id AND wy.year = cr.year
LEFT JOIN complaints co ON wy.ward_id = co.ward_id AND wy.year = co.year
LEFT JOIN aqi aq ON wy.ward_id = aq.ward_id AND wy.year = aq.year
LEFT JOIN census ce ON wy.ward_id = ce.ward_id AND wy.year = ce.year
LEFT JOIN transit tr ON wy.ward_id = tr.ward_id AND wy.year = tr.year
LEFT JOIN ndvi nd ON wy.ward_id = nd.ward_id AND wy.year = nd.year
LEFT JOIN property pr ON wy.ward_id = pr.ward_id AND wy.year = pr.year
