-- stg_aqi.sql
-- Staging: CPCB AQI data aggregated by ward + year
-- Maps AQI stations to wards using PostGIS ST_Contains

WITH station_ward_map AS (
    SELECT
        a.station_id,
        a.station_name,
        a.latitude,
        a.longitude,
        w.ward_id,
        w.ward_name
    FROM (
        SELECT DISTINCT station_id, station_name, latitude, longitude
        FROM {{ source('raw', 'cpcb_aqi') }}
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    ) a
    LEFT JOIN {{ source('raw', 'ward_boundaries') }} w
        ON ST_Contains(
            w.geom,
            ST_SetSRID(ST_MakePoint(a.longitude, a.latitude), 4326)
        )
),

aqi_annual AS (
    SELECT
        swm.ward_id,
        swm.ward_name,
        EXTRACT(YEAR FROM a.date)::INTEGER AS year,
        ROUND(AVG(a.pm25), 2) AS avg_pm25,
        ROUND(AVG(a.pm10), 2) AS avg_pm10,
        ROUND(AVG(a.no2), 2) AS avg_no2,
        ROUND(AVG(a.so2), 2) AS avg_so2,
        ROUND(AVG(a.co), 2) AS avg_co,
        ROUND(AVG(a.o3), 2) AS avg_o3,
        ROUND(AVG(a.aqi), 2) AS avg_aqi,
        -- AQI Good days: AQI <= 50 as per CPCB classification
        ROUND(
            COUNT(*) FILTER (WHERE a.aqi <= 50)::NUMERIC / NULLIF(COUNT(*), 0) * 100,
            2
        ) AS aqi_good_days_pct,
        COUNT(DISTINCT a.station_id) AS station_count,
        -- Representative station point (centroid of stations in ward)
        ST_SetSRID(
            ST_MakePoint(AVG(a.longitude), AVG(a.latitude)),
            4326
        ) AS geom
    FROM {{ source('raw', 'cpcb_aqi') }} a
    INNER JOIN station_ward_map swm
        ON a.station_id = swm.station_id
    WHERE a.date IS NOT NULL
      AND EXTRACT(YEAR FROM a.date) BETWEEN 2018 AND 2024
      AND swm.ward_id IS NOT NULL
    GROUP BY swm.ward_id, swm.ward_name, EXTRACT(YEAR FROM a.date)
)

SELECT
    ward_id,
    ward_name,
    year,
    avg_pm25,
    avg_pm10,
    avg_no2,
    avg_so2,
    avg_co,
    avg_o3,
    avg_aqi,
    aqi_good_days_pct,
    station_count,
    geom,
    NOW() AS updated_at
FROM aqi_annual
