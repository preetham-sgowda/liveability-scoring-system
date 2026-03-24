-- ============================================================
-- 005_feature_aggregation.sql
-- Aggregates raw data into marts.mart_ward_features
-- ============================================================

INSERT INTO marts.mart_ward_features (
    ward_id, ward_name, city, year,
    total_ipc_offenses, crime_rate_per_1000,
    total_complaints, resolution_rate,
    avg_aqi, population, population_density,
    avg_ndvi, median_price_sqft
)
SELECT 
    w.ward_id, w.ward_name, w.city, 2024,
    COALESCE(SUM(c.total_ipc_offenses), 0),
    COALESCE(AVG(c.total_ipc_offenses)::numeric / NULLIF(pop.population, 0) * 1000, 0),
    COALESCE(COUNT(comp.complaint_id), 0),
    COALESCE(SUM(CASE WHEN comp.status = 'Resolved' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(comp.complaint_id), 0), 0),
    COALESCE(AVG(aqi.aqi), 0),
    COALESCE(pop.population, 0),
    COALESCE(pop.density, 0),
    COALESCE(AVG(n.mean_ndvi), 0),
    COALESCE(AVG(p.price_sqft), 0)
FROM raw.ward_boundaries w
LEFT JOIN raw.ncrb_crime c ON w.ward_name = c.city -- Simplified join for demo
LEFT JOIN raw.civic_complaints comp ON w.ward_name = comp.ward_name AND w.city = comp.city
LEFT JOIN raw.cpcb_aqi aqi ON w.city = aqi.city
LEFT JOIN raw.census pop ON w.ward_name = pop.ward_name AND w.city = pop.city
LEFT JOIN raw.ndvi n ON w.ward_id = n.ward_id
LEFT JOIN raw.property_prices p ON w.ward_name = p.ward_name AND w.city = p.city
GROUP BY w.ward_id, w.ward_name, w.city, pop.population, pop.density
ON CONFLICT (ward_id, year) DO UPDATE SET
    total_ipc_offenses = EXCLUDED.total_ipc_offenses,
    crime_rate_per_1000 = EXCLUDED.crime_rate_per_1000,
    total_complaints = EXCLUDED.total_complaints,
    resolution_rate = EXCLUDED.resolution_rate,
    avg_aqi = EXCLUDED.avg_aqi,
    avg_ndvi = EXCLUDED.avg_ndvi,
    median_price_sqft = EXCLUDED.median_price_sqft;
