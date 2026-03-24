-- ============================================================
-- 005_seed_wards.sql
-- Seed ward boundary data from GeoJSON
-- Uses a placeholder approach — actual load via Python script
-- ============================================================

-- This file sets up the helper function for loading ward boundaries.
-- The actual ward polygon data is loaded by the census/ward boundary
-- ingestion script from data/ward_boundaries.geojson.

-- Function to load wards from GeoJSON file (called by Python loader)
CREATE OR REPLACE FUNCTION raw.upsert_ward_boundary(
    p_ward_name     VARCHAR,
    p_ward_number   INTEGER,
    p_city          VARCHAR,
    p_zone_name     VARCHAR,
    p_area_sqkm     NUMERIC,
    p_geojson       TEXT
) RETURNS INTEGER AS $$
DECLARE
    v_ward_id INTEGER;
BEGIN
    INSERT INTO raw.ward_boundaries (ward_name, ward_number, city, zone_name, area_sqkm, geom)
    VALUES (
        p_ward_name,
        p_ward_number,
        p_city,
        p_zone_name,
        p_area_sqkm,
        ST_SetSRID(ST_GeomFromGeoJSON(p_geojson), 4326)
    )
    ON CONFLICT (ward_number)
        DO UPDATE SET
            ward_name = EXCLUDED.ward_name,
            zone_name = EXCLUDED.zone_name,
            area_sqkm = EXCLUDED.area_sqkm,
            geom = EXCLUDED.geom
    RETURNING ward_id INTO v_ward_id;

    RETURN v_ward_id;
END;
$$ LANGUAGE plpgsql;

-- Add unique constraint on ward_number for upsert support
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_ward_boundaries_number'
    ) THEN
        ALTER TABLE raw.ward_boundaries
            ADD CONSTRAINT uq_ward_boundaries_number UNIQUE (ward_number);
    END IF;
END $$;
