-- ============================================================
-- 006_scoring_schema.sql
-- Mart layer — final liveability scores
-- ============================================================

CREATE TABLE IF NOT EXISTS marts.liveability_scores (
    ward_id             INTEGER NOT NULL REFERENCES raw.ward_boundaries(ward_id),
    ward_name           VARCHAR(200) NOT NULL,
    city                VARCHAR(100) NOT NULL,
    year                INTEGER NOT NULL,
    month               INTEGER NOT NULL,
    
    -- Composite Score
    composite_score     NUMERIC(5, 2) NOT NULL, -- 0-100
    
    -- Dimension Scores (0-100)
    safety_score        NUMERIC(5, 2),
    aqi_score           NUMERIC(5, 2),
    civic_score         NUMERIC(5, 2),
    transit_score       NUMERIC(5, 2),
    green_score         NUMERIC(5, 2),
    affordability_score NUMERIC(5, 2),
    
    -- Freshness
    data_freshness      JSONB, -- { "aqi": "2024-03-21T18:00:00Z", ... }
    
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ward_id, year, month)
);

CREATE INDEX IF NOT EXISTS idx_scores_city_ward ON marts.liveability_scores (city, ward_name);
