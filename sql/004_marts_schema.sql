-- ============================================================
-- 004_marts_schema.sql
-- Mart layer — final wide feature table for ML consumption
-- ============================================================

CREATE TABLE IF NOT EXISTS marts.mart_ward_features (
    -- ── Keys ──
    ward_id             INTEGER NOT NULL REFERENCES raw.ward_boundaries(ward_id),
    ward_name           VARCHAR(200) NOT NULL,
    ward_number         INTEGER,
    city                VARCHAR(100),
    zone_name           VARCHAR(100),
    year                INTEGER NOT NULL,

    -- ── Crime Features ──
    total_ipc_offenses  INTEGER,
    murder_count        INTEGER,
    theft_count         INTEGER,
    robbery_count       INTEGER,
    assault_count       INTEGER,
    kidnapping_count    INTEGER,
    burglary_count      INTEGER,
    crime_rate_per_1000 NUMERIC(8, 4),

    -- ── Civic Complaints Features ──
    total_complaints    INTEGER,
    pending_complaints  INTEGER,
    resolved_complaints INTEGER,
    resolution_rate     NUMERIC(5, 4),
    avg_resolution_days NUMERIC(8, 2),

    -- ── Air Quality Features ──
    avg_pm25            NUMERIC(8, 2),
    avg_pm10            NUMERIC(8, 2),
    avg_no2             NUMERIC(8, 2),
    avg_so2             NUMERIC(8, 2),
    avg_aqi             NUMERIC(8, 2),
    aqi_good_days_pct   NUMERIC(5, 2),

    -- ── Census / Demographics Features ──
    population          INTEGER,
    population_density  NUMERIC(10, 2),
    literacy_rate       NUMERIC(5, 2),
    avg_household_size  NUMERIC(5, 2),
    total_households    INTEGER,

    -- ── Transit Features ──
    bus_stops_count     INTEGER,
    metro_proximity_km  NUMERIC(8, 3),
    avg_route_frequency NUMERIC(8, 2),
    total_routes        INTEGER,

    -- ── Green Cover Features ──
    avg_ndvi            NUMERIC(6, 4),
    ndvi_change_yoy     NUMERIC(6, 4),
    green_cover_pct     NUMERIC(5, 2),

    -- ── Property Price (validation label) ──
    median_price_sqft   NUMERIC(10, 2),
    price_change_yoy    NUMERIC(8, 4),

    -- ── Geometry ──
    geom                GEOMETRY(MultiPolygon, 4326),

    -- ── Metadata ──
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (ward_id, year)
);

CREATE INDEX IF NOT EXISTS idx_mart_ward_year ON marts.mart_ward_features (ward_id, year);
CREATE INDEX IF NOT EXISTS idx_mart_geom ON marts.mart_ward_features USING GIST (geom);

-- Convenience view: latest year snapshot
CREATE OR REPLACE VIEW marts.v_ward_features_latest AS
SELECT *
FROM marts.mart_ward_features
WHERE year = (SELECT MAX(year) FROM marts.mart_ward_features);
