-- ============================================================
-- 003_staging_schema.sql
-- Staging layer — cleaned, typed, ward-joined tables
-- ============================================================

-- ── Staging: Crime (aggregated by city + year) ──
CREATE TABLE IF NOT EXISTS staging.stg_crime (
    id              SERIAL PRIMARY KEY,
    ward_id         INTEGER REFERENCES raw.ward_boundaries(ward_id),
    ward_name       VARCHAR(200),
    city            VARCHAR(100),
    year            INTEGER NOT NULL,
    total_ipc_offenses  INTEGER DEFAULT 0,
    murder_count    INTEGER DEFAULT 0,
    theft_count     INTEGER DEFAULT 0,
    robbery_count   INTEGER DEFAULT 0,
    assault_count   INTEGER DEFAULT 0,
    kidnapping_count INTEGER DEFAULT 0,
    burglary_count  INTEGER DEFAULT 0,
    crime_rate_per_1000 NUMERIC(8, 4),
    source_pdf      VARCHAR(500),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ward_id, year)
);

-- ── Staging: BBMP Complaints (aggregated by ward + year) ──
CREATE TABLE IF NOT EXISTS staging.stg_complaints (
    id              SERIAL PRIMARY KEY,
    ward_id         INTEGER REFERENCES raw.ward_boundaries(ward_id),
    ward_name       VARCHAR(200),
    year            INTEGER NOT NULL,
    total_complaints    INTEGER DEFAULT 0,
    pending_complaints  INTEGER DEFAULT 0,
    resolved_complaints INTEGER DEFAULT 0,
    rejected_complaints INTEGER DEFAULT 0,
    resolution_rate NUMERIC(5, 4),
    avg_resolution_days NUMERIC(8, 2),
    top_category    VARCHAR(200),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ward_id, year)
);

-- ── Staging: AQI (annual averages per ward) ──
CREATE TABLE IF NOT EXISTS staging.stg_aqi (
    id              SERIAL PRIMARY KEY,
    ward_id         INTEGER REFERENCES raw.ward_boundaries(ward_id),
    ward_name       VARCHAR(200),
    year            INTEGER NOT NULL,
    avg_pm25        NUMERIC(8, 2),
    avg_pm10        NUMERIC(8, 2),
    avg_no2         NUMERIC(8, 2),
    avg_so2         NUMERIC(8, 2),
    avg_co          NUMERIC(8, 2),
    avg_o3          NUMERIC(8, 2),
    avg_aqi         NUMERIC(8, 2),
    aqi_good_days_pct   NUMERIC(5, 2),
    station_count   INTEGER,
    geom            GEOMETRY(Point, 4326),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ward_id, year)
);

-- ── Staging: Census ──
CREATE TABLE IF NOT EXISTS staging.stg_census (
    id              SERIAL PRIMARY KEY,
    ward_id         INTEGER REFERENCES raw.ward_boundaries(ward_id),
    ward_name       VARCHAR(200),
    year            INTEGER NOT NULL,
    population      INTEGER,
    population_density  NUMERIC(10, 2),
    literacy_rate   NUMERIC(5, 2),
    male_literacy   NUMERIC(5, 2),
    female_literacy NUMERIC(5, 2),
    avg_household_size  NUMERIC(5, 2),
    total_households INTEGER,
    sex_ratio       NUMERIC(8, 2),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ward_id, year)
);

-- ── Staging: Transit ──
CREATE TABLE IF NOT EXISTS staging.stg_transit (
    id              SERIAL PRIMARY KEY,
    ward_id         INTEGER REFERENCES raw.ward_boundaries(ward_id),
    ward_name       VARCHAR(200),
    year            INTEGER NOT NULL,
    bus_stops_count INTEGER DEFAULT 0,
    metro_proximity_km  NUMERIC(8, 3),
    avg_route_frequency NUMERIC(8, 2),
    total_routes    INTEGER DEFAULT 0,
    transit_score   NUMERIC(5, 2),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ward_id, year)
);

-- ── Staging: NDVI ──
CREATE TABLE IF NOT EXISTS staging.stg_ndvi (
    id              SERIAL PRIMARY KEY,
    ward_id         INTEGER REFERENCES raw.ward_boundaries(ward_id),
    ward_name       VARCHAR(200),
    year            INTEGER NOT NULL,
    avg_ndvi        NUMERIC(6, 4),
    median_ndvi     NUMERIC(6, 4),
    min_ndvi        NUMERIC(6, 4),
    max_ndvi        NUMERIC(6, 4),
    ndvi_change_yoy NUMERIC(6, 4),
    green_cover_pct NUMERIC(5, 2),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ward_id, year)
);

-- ── Staging: Property Prices ──
CREATE TABLE IF NOT EXISTS staging.stg_property_prices (
    id              SERIAL PRIMARY KEY,
    ward_id         INTEGER REFERENCES raw.ward_boundaries(ward_id),
    ward_name       VARCHAR(200),
    year            INTEGER NOT NULL,
    median_price_sqft   NUMERIC(10, 2),
    avg_price_sqft      NUMERIC(10, 2),
    min_price_sqft      NUMERIC(10, 2),
    max_price_sqft      NUMERIC(10, 2),
    listing_count       INTEGER DEFAULT 0,
    price_change_yoy    NUMERIC(8, 4),
    dominant_property_type VARCHAR(100),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ward_id, year)
);
