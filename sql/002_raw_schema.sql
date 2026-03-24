-- ============================================================
-- 002_raw_schema.sql
-- Raw layer tables — exact dumps from all 7 data sources
-- ============================================================

-- Ward boundaries reference table (BBMP 198 wards)
CREATE TABLE IF NOT EXISTS raw.ward_boundaries (
    ward_id         SERIAL PRIMARY KEY,
    ward_name       VARCHAR(200) NOT NULL,
    ward_number     INTEGER,
    city            VARCHAR(100) NOT NULL,
    zone_name       VARCHAR(100),
    area_sqkm       NUMERIC(10, 4),
    geom            GEOMETRY(MultiPolygon, 4326) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ward_boundaries_geom ON raw.ward_boundaries USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_ward_boundaries_name ON raw.ward_boundaries USING GIN (ward_name gin_trgm_ops);

-- ── 1. NCRB Crime ──
CREATE TABLE IF NOT EXISTS raw.ncrb_crime (
    id              SERIAL PRIMARY KEY,
    state           VARCHAR(100),
    city            VARCHAR(100),
    year            INTEGER NOT NULL,
    offense_category VARCHAR(200),
    offense_type    VARCHAR(300),
    count           INTEGER DEFAULT 0,
    source_pdf      VARCHAR(500),
    page_number     INTEGER,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ncrb_crime_city_year ON raw.ncrb_crime (city, year);

-- ── 2. Unified Civic Complaints ──
CREATE TABLE IF NOT EXISTS raw.civic_complaints (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    complaint_id    VARCHAR(50),
    category        VARCHAR(200),
    subcategory     VARCHAR(200),
    ward_name       VARCHAR(200),
    ward_number     INTEGER,
    date_filed      DATE,
    status          VARCHAR(50),
    description     TEXT,
    latitude        NUMERIC(10, 7),
    longitude       NUMERIC(10, 7),
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (city, complaint_id)
);
CREATE INDEX IF NOT EXISTS idx_civic_complaints_city_ward ON raw.civic_complaints (city, ward_name);
CREATE INDEX IF NOT EXISTS idx_civic_complaints_date ON raw.civic_complaints (date_filed);

-- Legacy BBMP Sahaaya Complaints (kept for backward compatibility or migrated)
CREATE TABLE IF NOT EXISTS raw.bbmp_complaints (
    id              SERIAL PRIMARY KEY,
    complaint_id    VARCHAR(50) UNIQUE,
    category        VARCHAR(200),
    subcategory     VARCHAR(200),
    ward_name       VARCHAR(200),
    ward_number     INTEGER,
    date_filed      DATE,
    status          VARCHAR(50),
    description     TEXT,
    latitude        NUMERIC(10, 7),
    longitude       NUMERIC(10, 7),
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_bbmp_complaints_ward ON raw.bbmp_complaints (ward_name);
CREATE INDEX IF NOT EXISTS idx_bbmp_complaints_date ON raw.bbmp_complaints (date_filed);

-- ── 3. CPCB AQI ──
CREATE TABLE IF NOT EXISTS raw.cpcb_aqi (
    id              SERIAL PRIMARY KEY,
    station_id      VARCHAR(50),
    station_name    VARCHAR(200),
    city            VARCHAR(100) DEFAULT 'Bengaluru',
    latitude        NUMERIC(10, 7),
    longitude       NUMERIC(10, 7),
    date            DATE NOT NULL,
    pm25            NUMERIC(8, 2),
    pm10            NUMERIC(8, 2),
    no2             NUMERIC(8, 2),
    so2             NUMERIC(8, 2),
    co              NUMERIC(8, 2),
    o3              NUMERIC(8, 2),
    aqi             INTEGER,
    prominent_pollutant VARCHAR(20),
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (station_id, date)
);
CREATE INDEX IF NOT EXISTS idx_cpcb_aqi_station_date ON raw.cpcb_aqi (station_id, date);

-- ── 4. Census of India ──
CREATE TABLE IF NOT EXISTS raw.census (
    id              SERIAL PRIMARY KEY,
    ward_name       VARCHAR(200),
    ward_number     INTEGER,
    city            VARCHAR(100),
    population      INTEGER,
    male_population INTEGER,
    female_population INTEGER,
    area_sqkm       NUMERIC(10, 4),
    density         NUMERIC(10, 2),
    literacy_rate   NUMERIC(5, 2),
    male_literacy   NUMERIC(5, 2),
    female_literacy NUMERIC(5, 2),
    household_size  NUMERIC(5, 2),
    total_households INTEGER,
    year            INTEGER NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_census_ward ON raw.census (ward_name);

-- ── 5. GTFS Transit Stops ──
CREATE TABLE IF NOT EXISTS raw.gtfs_stops (
    id              SERIAL PRIMARY KEY,
    stop_id         VARCHAR(50),
    stop_name       VARCHAR(300),
    stop_desc       TEXT,
    latitude        NUMERIC(10, 7) NOT NULL,
    longitude       NUMERIC(10, 7) NOT NULL,
    zone_id         VARCHAR(50),
    route_count     INTEGER DEFAULT 0,
    avg_frequency   NUMERIC(8, 2),
    source          VARCHAR(50) DEFAULT 'BMTC',
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_gtfs_stops_location ON raw.gtfs_stops (latitude, longitude);

-- ── 6. NDVI (Sentinel-2 via GEE) ──
CREATE TABLE IF NOT EXISTS raw.ndvi (
    id              SERIAL PRIMARY KEY,
    ward_id         INTEGER REFERENCES raw.ward_boundaries(ward_id),
    ward_name       VARCHAR(200),
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    mean_ndvi       NUMERIC(6, 4),
    median_ndvi     NUMERIC(6, 4),
    min_ndvi        NUMERIC(6, 4),
    max_ndvi        NUMERIC(6, 4),
    std_ndvi        NUMERIC(6, 4),
    pixel_count     INTEGER,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ward_id, year, month)
);
CREATE INDEX IF NOT EXISTS idx_ndvi_ward_year ON raw.ndvi (ward_id, year);

-- ── 7. Property Prices (MagicBricks / 99acres) ──
CREATE TABLE IF NOT EXISTS raw.property_prices (
    id              SERIAL PRIMARY KEY,
    listing_id      VARCHAR(100),
    source          VARCHAR(50) NOT NULL,       -- 'magicbricks' or '99acres'
    locality        VARCHAR(200),
    ward_name       VARCHAR(200),
    city            VARCHAR(100) DEFAULT 'Bengaluru',
    price_total     NUMERIC(14, 2),
    price_sqft      NUMERIC(10, 2),
    area_sqft       NUMERIC(10, 2),
    bedrooms        INTEGER,
    property_type   VARCHAR(100),
    listing_date    DATE,
    scraped_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_property_ward ON raw.property_prices (ward_name);
CREATE INDEX IF NOT EXISTS idx_property_date ON raw.property_prices (listing_date);

-- ── Pipeline Runs (audit log) ──
CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    run_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dag_id          VARCHAR(100) NOT NULL,
    task_id         VARCHAR(100),
    execution_date  TIMESTAMPTZ,
    status          VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    records_loaded  INTEGER DEFAULT 0,
    error_message   TEXT,
    metadata        JSONB
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag ON raw.pipeline_runs (dag_id, started_at DESC);
