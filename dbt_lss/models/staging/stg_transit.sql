-- stg_transit.sql
-- Staging: GTFS transit data aggregated by ward + year
-- Maps bus stops to wards using PostGIS, computes frequency metrics

WITH stop_ward_map AS (
    SELECT
        s.stop_id,
        s.stop_name,
        s.latitude,
        s.longitude,
        s.route_count,
        s.avg_frequency,
        s.source,
        w.ward_id,
        w.ward_name,
        -- Distance to nearest metro station
        (
            SELECT MIN(
                ST_Distance(
                    ST_SetSRID(ST_MakePoint(s.longitude, s.latitude), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(m.longitude, m.latitude), 4326)::geography
                ) / 1000.0
            )
            FROM {{ source('raw', 'gtfs_stops') }} m
            WHERE m.source = 'BMRCL'
        ) AS metro_distance_km
    FROM {{ source('raw', 'gtfs_stops') }} s
    LEFT JOIN {{ source('raw', 'ward_boundaries') }} w
        ON ST_Contains(
            w.geom,
            ST_SetSRID(ST_MakePoint(s.longitude, s.latitude), 4326)
        )
    WHERE s.latitude IS NOT NULL
      AND s.longitude IS NOT NULL
),

ward_transit AS (
    SELECT
        ward_id,
        ward_name,
        COUNT(*) AS bus_stops_count,
        MIN(metro_distance_km) AS metro_proximity_km,
        ROUND(AVG(avg_frequency), 2) AS avg_route_frequency,
        SUM(route_count) AS total_routes
    FROM stop_ward_map
    WHERE ward_id IS NOT NULL
    GROUP BY ward_id, ward_name
)

-- Transit data is relatively static, replicate across years
SELECT
    wt.ward_id,
    wt.ward_name,
    y.year,
    wt.bus_stops_count,
    ROUND(COALESCE(wt.metro_proximity_km, 99.0), 3) AS metro_proximity_km,
    wt.avg_route_frequency,
    wt.total_routes,
    -- Simple transit accessibility score (0-100)
    ROUND(
        LEAST(100, (
            (LEAST(wt.bus_stops_count, 50) / 50.0 * 40) +
            (GREATEST(0, 1 - wt.metro_proximity_km / 10.0) * 40) +
            (LEAST(wt.avg_route_frequency, 100) / 100.0 * 20)
        )), 2
    ) AS transit_score,
    NOW() AS updated_at
FROM ward_transit wt
CROSS JOIN (
    SELECT generate_series(2018, 2024) AS year
) y
