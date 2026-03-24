"""
gtfs_loader.py — GTFS transit data ingestion.

Parses GTFS feed files (stops.txt, routes.txt, stop_times.txt) from
BMTC (Bengaluru Metropolitan Transport Corporation) and computes:
  - Bus stops per ward (via PostGIS spatial join)
  - Route frequency per stop
  - Metro proximity (from BMRCL metro stations)

Also supports loading metro station data from BMRCL GTFS.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def parse_gtfs_stops(gtfs_dir: str,
                     source: str = "BMTC") -> pd.DataFrame:
    """
    Parse stops.txt from a GTFS feed directory.

    Args:
        gtfs_dir: Path to directory containing GTFS txt files
        source: Source identifier ('BMTC' or 'BMRCL')

    Returns:
        DataFrame with stop_id, stop_name, latitude, longitude, source
    """
    gtfs_path = Path(gtfs_dir)
    stops_file = gtfs_path / "stops.txt"

    if not stops_file.exists():
        raise FileNotFoundError(f"stops.txt not found in {gtfs_dir}")

    logger.info(f"Parsing stops.txt from {gtfs_dir} (source={source})")

    df = pd.read_csv(str(stops_file))

    # Normalize column names
    df.columns = [c.lower().strip() for c in df.columns]

    # Map GTFS standard columns
    column_map = {
        "stop_lat": "latitude",
        "stop_lon": "longitude",
    }
    df.rename(columns=column_map, inplace=True)

    # Ensure required columns
    required = ["stop_id", "stop_name", "latitude", "longitude"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required GTFS columns: {missing}")

    # Clean data
    df["stop_id"] = df["stop_id"].astype(str).str.strip()
    df["stop_name"] = df["stop_name"].str.strip()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["source"] = source

    # Filter to city bounding box
    from scripts.geo.utils import CITY_BBOXES
    city_name = "Bengaluru" # Default
    if "BMTC" in source or "BMRCL" in source: city_name = "Bengaluru"
    elif "BEST" in source or "MMRDA" in source: city_name = "Mumbai"
    elif "DTC" in source or "DMRC" in source: city_name = "Delhi"
    
    bbox = CITY_BBOXES.get(city_name)
    mask = (
        (df["latitude"] >= bbox["south"]) &
        (df["latitude"] <= bbox["north"]) &
        (df["longitude"] >= bbox["west"]) &
        (df["longitude"] <= bbox["east"])
    )
    filtered = df[mask].copy()
    logger.info(
        f"Filtered {len(filtered)}/{len(df)} stops within {city_name} bbox"
    )

    # Include stop_desc if available
    if "stop_desc" in df.columns:
        filtered["stop_desc"] = filtered["stop_desc"].fillna("")
    else:
        filtered["stop_desc"] = ""

    if "zone_id" in df.columns:
        filtered["zone_id"] = filtered["zone_id"].astype(str)
    else:
        filtered["zone_id"] = ""

    return filtered


def compute_route_frequency(gtfs_dir: str) -> pd.DataFrame:
    """
    Compute route count and average frequency per stop from
    stop_times.txt and trips.txt.

    Frequency = number of trips per day passing through each stop.

    Args:
        gtfs_dir: Path to GTFS directory

    Returns:
        DataFrame with stop_id, route_count, avg_frequency
    """
    gtfs_path = Path(gtfs_dir)
    stop_times_file = gtfs_path / "stop_times.txt"
    trips_file = gtfs_path / "trips.txt"

    if not stop_times_file.exists():
        logger.warning("stop_times.txt not found, skipping frequency calc")
        return pd.DataFrame(columns=["stop_id", "route_count", "avg_frequency"])

    logger.info("Computing route frequency from stop_times.txt")

    stop_times = pd.read_csv(str(stop_times_file), usecols=[
        "trip_id", "stop_id"
    ])
    stop_times.columns = [c.lower().strip() for c in stop_times.columns]

    # If trips.txt exists, get route_id mapping
    if trips_file.exists():
        trips = pd.read_csv(str(trips_file), usecols=["trip_id", "route_id"])
        trips.columns = [c.lower().strip() for c in trips.columns]
        merged = stop_times.merge(trips, on="trip_id", how="left")

        # Routes per stop
        route_counts = (
            merged.groupby("stop_id")["route_id"]
            .nunique()
            .reset_index()
            .rename(columns={"route_id": "route_count"})
        )
    else:
        route_counts = (
            stop_times.groupby("stop_id")["trip_id"]
            .nunique()
            .reset_index()
            .rename(columns={"trip_id": "route_count"})
        )

    # Trips per stop (proxy for frequency)
    trip_counts = (
        stop_times.groupby("stop_id")["trip_id"]
        .count()
        .reset_index()
        .rename(columns={"trip_id": "avg_frequency"})
    )

    result = route_counts.merge(trip_counts, on="stop_id", how="outer")
    result["stop_id"] = result["stop_id"].astype(str)

    logger.info(f"Computed frequency for {len(result)} stops")
    return result


def prepare_stops_for_load(stops_df: pd.DataFrame,
                           frequency_df: Optional[pd.DataFrame] = None
                           ) -> pd.DataFrame:
    """
    Merge stops with frequency data and prepare for DB loading.

    Args:
        stops_df: Parsed stops DataFrame
        frequency_df: Route frequency DataFrame (optional)

    Returns:
        Merged DataFrame ready for insertion
    """
    if frequency_df is not None and not frequency_df.empty:
        merged = stops_df.merge(frequency_df, on="stop_id", how="left")
        merged["route_count"] = merged["route_count"].fillna(0).astype(int)
        merged["avg_frequency"] = merged["avg_frequency"].fillna(0)
    else:
        merged = stops_df.copy()
        merged["route_count"] = 0
        merged["avg_frequency"] = 0.0

    return merged


def get_prepare_rows(df: pd.DataFrame) -> tuple[list[str], list[tuple]]:
    """
    Prepare stops DataFrame for bulk DB insertion into raw.gtfs_stops.

    Returns:
        Tuple of (column_names, row_tuples)
    """
    columns = [
        "stop_id", "stop_name", "stop_desc", "latitude", "longitude",
        "zone_id", "route_count", "avg_frequency", "source"
    ]
    rows = []
    for _, row in df.iterrows():
        rows.append((
            str(row.get("stop_id", "")),
            str(row.get("stop_name", "")),
            str(row.get("stop_desc", "")),
            row.get("latitude"),
            row.get("longitude"),
            str(row.get("zone_id", "")),
            int(row.get("route_count", 0)),
            float(row.get("avg_frequency", 0)),
            str(row.get("source", "BMTC")),
        ))
    return columns, rows
