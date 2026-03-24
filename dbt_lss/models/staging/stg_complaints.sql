-- stg_complaints.sql
-- Staging: BBMP Sahaaya complaints aggregated by ward + year

WITH complaint_agg AS (
    SELECT
        ward_name,
        EXTRACT(YEAR FROM date_filed)::INTEGER AS year,
        COUNT(*) AS total_complaints,
        COUNT(*) FILTER (WHERE LOWER(status) IN ('pending', 'open', 'in progress', 'assigned')) AS pending_complaints,
        COUNT(*) FILTER (WHERE LOWER(status) IN ('resolved', 'closed', 'completed')) AS resolved_complaints,
        COUNT(*) FILTER (WHERE LOWER(status) IN ('rejected', 'cancelled')) AS rejected_complaints,
        -- Resolution rate
        CASE
            WHEN COUNT(*) > 0 THEN
                COUNT(*) FILTER (WHERE LOWER(status) IN ('resolved', 'closed', 'completed'))::NUMERIC / COUNT(*)
            ELSE 0
        END AS resolution_rate,
        -- Most common category
        MODE() WITHIN GROUP (ORDER BY category) AS top_category
    FROM {{ source('raw', 'bbmp_complaints') }}
    WHERE date_filed IS NOT NULL
      AND EXTRACT(YEAR FROM date_filed) BETWEEN 2018 AND 2024
    GROUP BY ward_name, EXTRACT(YEAR FROM date_filed)
),

wards AS (
    SELECT ward_id, ward_name
    FROM {{ source('raw', 'ward_boundaries') }}
)

SELECT
    w.ward_id,
    w.ward_name,
    c.year,
    COALESCE(c.total_complaints, 0) AS total_complaints,
    COALESCE(c.pending_complaints, 0) AS pending_complaints,
    COALESCE(c.resolved_complaints, 0) AS resolved_complaints,
    COALESCE(c.rejected_complaints, 0) AS rejected_complaints,
    COALESCE(c.resolution_rate, 0)::NUMERIC(5, 4) AS resolution_rate,
    NULL::NUMERIC(8, 2) AS avg_resolution_days,  -- Requires date_resolved field
    c.top_category,
    NOW() AS updated_at
FROM wards w
INNER JOIN complaint_agg c
    ON LOWER(TRIM(w.ward_name)) = LOWER(TRIM(c.ward_name))
