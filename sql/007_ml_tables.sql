-- ============================================================
-- 007_ml_tables.sql
-- ML output tables + enriched feature columns
-- ============================================================

-- ── Ward Clusters (KMeans output) ──
CREATE TABLE IF NOT EXISTS marts.ward_clusters (
    ward_id         INTEGER NOT NULL REFERENCES raw.ward_boundaries(ward_id),
    year            INTEGER NOT NULL,
    cluster_id      INTEGER NOT NULL,
    cluster_label   VARCHAR(100) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ward_id, year)
);

CREATE INDEX IF NOT EXISTS idx_ward_clusters_label
    ON marts.ward_clusters (cluster_label);

-- ── Decline Predictions (XGBoost output) ──
CREATE TABLE IF NOT EXISTS marts.ward_decline_predictions (
    ward_id             INTEGER NOT NULL REFERENCES raw.ward_boundaries(ward_id),
    year                INTEGER NOT NULL,
    decline_probability NUMERIC(5, 4),
    decline_predicted   BOOLEAN NOT NULL DEFAULT FALSE,
    model_version       VARCHAR(50),
    predicted_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ward_id, year)
);

CREATE INDEX IF NOT EXISTS idx_decline_pred_prob
    ON marts.ward_decline_predictions (decline_probability DESC);

-- ── SHAP Drivers (per-ward top-3 explainability) ──
CREATE TABLE IF NOT EXISTS marts.ward_shap_drivers (
    ward_id             INTEGER NOT NULL REFERENCES raw.ward_boundaries(ward_id),
    year                INTEGER NOT NULL,
    driver_1_feature    VARCHAR(100),
    driver_1_value      NUMERIC(10, 6),
    driver_2_feature    VARCHAR(100),
    driver_2_value      NUMERIC(10, 6),
    driver_3_feature    VARCHAR(100),
    driver_3_value      NUMERIC(10, 6),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ward_id, year)
);

-- ── Add derived columns to mart_ward_features ──
ALTER TABLE marts.mart_ward_features
    ADD COLUMN IF NOT EXISTS crime_trend_3y        NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS aqi_trend_3y          NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS ndvi_trend_3y         NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS complaint_backlog_pct NUMERIC(5, 4),
    ADD COLUMN IF NOT EXISTS transit_walkability   NUMERIC(5, 2),
    ADD COLUMN IF NOT EXISTS yoy_price_change_3y_avg NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS cluster_id            INTEGER,
    ADD COLUMN IF NOT EXISTS cluster_label         VARCHAR(100);
