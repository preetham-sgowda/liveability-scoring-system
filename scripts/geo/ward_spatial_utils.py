"""
ward_spatial_utils.py — PostGIS spatial join utilities.

Provides helpers for:
  - Point-in-polygon ward lookup
  - Batch ward assignment for DataFrames with lat/lon
  - Nearest-ward fallback for points outside boundaries
"""

import logging
from typing import Optional

import pandas as pd

from scripts.db_utils import get_db_connection

logger = logging.getLogger(__name__)


def get_ward_id(lat: float, lon: float) -> Optional[int]:
    """
    Find the ward_id for a given latitude/longitude using PostGIS ST_Contains.

    Falls back to ST_DWithin (500m buffer) if point is on a boundary edge.

    Args:
        lat: Latitude (WGS84)
        lon: Longitude (WGS84)

    Returns:
        ward_id or None if no ward found
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Exact containment
            cur.execute("""
                SELECT ward_id FROM raw.ward_boundaries
                WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                LIMIT 1
            """, (lon, lat))
            row = cur.fetchone()

            if row:
                return row[0]

            # Fallback: nearest within 500m
            cur.execute("""
                SELECT ward_id FROM raw.ward_boundaries
                WHERE ST_DWithin(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    500
                )
                ORDER BY ST_Distance(
                    geom::geography,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                )
                LIMIT 1
            """, (lon, lat, lon, lat))
            row = cur.fetchone()

            return row[0] if row else None


def spatial_join_df(df: pd.DataFrame,
                    lat_col: str = "latitude",
                    lon_col: str = "longitude") -> pd.DataFrame:
    """
    Batch assign ward_id to a DataFrame using PostGIS spatial join.

    Much faster than row-by-row lookup — uses a single SQL query with
    unnested arrays.

    Args:
        df: DataFrame with latitude and longitude columns
        lat_col: Name of latitude column
        lon_col: Name of longitude column

    Returns:
        DataFrame with added 'ward_id' and 'ward_name' columns
    """
    if df.empty:
        df["ward_id"] = None
        df["ward_name"] = None
        return df

    # Filter rows with valid coordinates
    valid_mask = df[lat_col].notna() & df[lon_col].notna()
    coords = df.loc[valid_mask, [lat_col, lon_col]].values.tolist()

    if not coords:
        df["ward_id"] = None
        df["ward_name"] = None
        return df

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Build batch lookup query
            values = ", ".join(
                f"({i}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))"
                for i, (lat, lon) in enumerate(coords)
            )

            cur.execute(f"""
                WITH points AS (
                    SELECT idx, pt
                    FROM (VALUES {values}) AS t(idx, pt)
                )
                SELECT
                    p.idx,
                    w.ward_id,
                    w.ward_name
                FROM points p
                LEFT JOIN raw.ward_boundaries w
                    ON ST_Contains(w.geom, p.pt)
            """)

            results = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

    # Map results back to DataFrame
    valid_indices = df.loc[valid_mask].index.tolist()
    ward_ids = []
    ward_names = []

    for i, idx in enumerate(valid_indices):
        ward_id, ward_name = results.get(i, (None, None))
        ward_ids.append(ward_id)
        ward_names.append(ward_name)

    df.loc[valid_mask, "ward_id"] = ward_ids
    df.loc[valid_mask, "ward_name"] = ward_names
    df.loc[~valid_mask, "ward_id"] = None
    df.loc[~valid_mask, "ward_name"] = None

    matched = sum(1 for wid in ward_ids if wid is not None)
    logger.info(
        f"Spatial join: {matched}/{len(coords)} points matched to wards"
    )

    return df


def get_metro_proximity(ward_id: int) -> Optional[float]:
    """
    Calculate distance in km from ward centroid to nearest metro station.

    Args:
        ward_id: Ward identifier

    Returns:
        Distance in km or None
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Use GTFS stops with source = 'BMRCL' (metro)
            cur.execute("""
                SELECT MIN(
                    ST_Distance(
                        w.geom::geography,
                        ST_SetSRID(ST_MakePoint(s.longitude, s.latitude), 4326)::geography
                    ) / 1000.0
                ) AS distance_km
                FROM raw.ward_boundaries w
                CROSS JOIN raw.gtfs_stops s
                WHERE w.ward_id = %s
                  AND s.source = 'BMRCL'
            """, (ward_id,))
            row = cur.fetchone()
            return round(row[0], 3) if row and row[0] else None
