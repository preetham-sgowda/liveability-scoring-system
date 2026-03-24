"""
ndvi_pipeline.py — Google Earth Engine NDVI pipeline.

Computes monthly NDVI (Normalized Difference Vegetation Index) from
Sentinel-2 imagery via Google Earth Engine API, then aggregates
raster values to BBMP ward polygons using zonal statistics.

Requires:
  - GEE service account authentication
  - Ward boundary GeoJSON or PostGIS table
"""

import os
import json
import logging
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# City bounding boxes
CITY_BBOXES = {
    "Bengaluru": {"west": 77.35, "south": 12.73, "east": 77.85, "north": 13.18},
    "Mumbai": {"west": 72.77, "south": 18.89, "east": 72.98, "north": 19.27},
    "Delhi": {"west": 76.84, "south": 28.41, "east": 77.35, "north": 28.88},
}

# Scale for export in meters
EXPORT_SCALE = 10  # Sentinel-2 native 10m resolution


def authenticate_gee(service_account: Optional[str] = None,
                     key_file: Optional[str] = None):
    """
    Authenticate with Google Earth Engine using a service account.

    Args:
        service_account: GEE service account email
        key_file: Path to the JSON key file
    """
    import ee

    service_account = service_account or os.getenv("GEE_SERVICE_ACCOUNT")
    key_file = key_file or os.getenv("GEE_KEY_FILE")

    if service_account and key_file:
        credentials = ee.ServiceAccountCredentials(service_account, key_file)
        ee.Initialize(credentials)
        logger.info(f"GEE authenticated with service account: {service_account}")
    else:
        # Fall back to interactive auth (for local dev)
        ee.Authenticate()
        ee.Initialize()
        logger.info("GEE authenticated interactively")


def compute_ndvi_composite(year: int, month: int, city: str = "Bengaluru",
                           recursion_depth: int = 0) -> "ee.Image":
    """
    Compute monthly NDVI composite from Sentinel-2 SR imagery.
    Recursive fallback to previous month if cloud cover > 80%.
    """
    import ee

    if recursion_depth > 3: # Limit fallback to 3 months
        logger.warning(f"Max recursion depth reached for {city} NDVI. Using latest available.")
        # return empty or latest

    # Define date range
    start_date = f"{year}-{month:02d}-01"
    if month == 12:
        end_date = f"{year + 1}-01-01"
    else:
        end_date = f"{year}-{month + 1:02d}-01"

    bbox = CITY_BBOXES.get(city, CITY_BBOXES["Bengaluru"])
    roi = ee.Geometry.Rectangle([bbox["west"], bbox["south"], bbox["east"], bbox["north"]])

    # Load Sentinel-2 Surface Reflectance
    s2 = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start_date, end_date)
        .filterBounds(roi)
    )

    # Check cloud cover
    avg_cloud = s2.aggregate_mean("CLOUDY_PIXEL_PERCENTAGE").getInfo()
    if avg_cloud is None or avg_cloud > 80:
        logger.warning(f"Cloud cover {avg_cloud}% too high for {year}-{month}. Falling back.")
        prev_month = month - 1 if month > 1 else 12
        prev_year = year if month > 1 else year - 1
        return compute_ndvi_composite(prev_year, prev_month, city, recursion_depth + 1)

    # Cloud masking using SCL
    def mask_clouds(image):
        scl = image.select("SCL")
        # Keep vegetation, bare soil, water pixels
        mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
        return image.updateMask(mask)

    s2_masked = s2.filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 30)).map(mask_clouds)

    # Compute NDVI
    def add_ndvi(image):
        ndvi = image.normalizedDifference(["B8", "B4"]).rename("NDVI")
        return image.addBands(ndvi)

    s2_ndvi = s2_masked.map(add_ndvi)

    # Median composite
    composite = s2_ndvi.select("NDVI").median().clip(roi)

    logger.info(f"NDVI composite for {city} {year}-{month} ({s2.size().getInfo()} scenes)")
    return composite


def export_ndvi_raster(ndvi_image: "ee.Image", year: int, month: int,
                       output_dir: str, city: str = "Bengaluru") -> str:
    """
    Export NDVI raster as GeoTIFF via GEE.

    Args:
        ndvi_image: ee.Image with NDVI band
        year: Year
        month: Month
        output_dir: Directory to save the raster

    Returns:
        Path to the exported GeoTIFF
    """
    import ee
    import requests as req

    output_path = Path(output_dir) / f"ndvi_{city}_{year}_{month:02d}.tif"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    bbox = CITY_BBOXES.get(city, CITY_BBOXES["Bengaluru"])
    roi = ee.Geometry.Rectangle([bbox["west"], bbox["south"], bbox["east"], bbox["north"]])

    # Get download URL
    url = ndvi_image.getDownloadURL({
        "scale": EXPORT_SCALE,
        "crs": "EPSG:4326",
        "region": roi,
        "format": "GEO_TIFF",
    })

    # Download
    response = req.get(url, timeout=300)
    response.raise_for_status()

    with open(str(output_path), "wb") as f:
        f.write(response.content)

    logger.info(f"Exported NDVI raster: {output_path}")
    return str(output_path)


def aggregate_ndvi_to_wards(raster_path: str,
                            ward_geojson_path: str,
                            year: int,
                            month: int) -> list[dict]:
    """
    Aggregate NDVI raster values to ward polygons using zonal statistics.

    Args:
        raster_path: Path to NDVI GeoTIFF
        ward_geojson_path: Path to ward boundaries GeoJSON
        year: Year
        month: Month

    Returns:
        List of dicts with ward-level NDVI statistics
    """
    import rasterio
    from rasterstats import zonal_stats
    import geopandas as gpd

    logger.info(f"Aggregating NDVI to wards from {raster_path}")

    # Load ward boundaries
    wards = gpd.read_file(ward_geojson_path)

    # Compute zonal statistics
    stats = zonal_stats(
        wards,
        raster_path,
        stats=["mean", "median", "min", "max", "std", "count"],
        nodata=-9999,
    )

    records = []
    for ward_idx, stat in enumerate(stats):
        if stat["count"] == 0 or stat["mean"] is None:
            continue

        ward = wards.iloc[ward_idx]
        records.append({
            "ward_id": ward.get("ward_id", ward_idx + 1),
            "ward_name": ward.get("ward_name", ward.get("name", "")),
            "year": year,
            "month": month,
            "mean_ndvi": round(stat["mean"], 4) if stat["mean"] else None,
            "median_ndvi": round(stat["median"], 4) if stat["median"] else None,
            "min_ndvi": round(stat["min"], 4) if stat["min"] else None,
            "max_ndvi": round(stat["max"], 4) if stat["max"] else None,
            "std_ndvi": round(stat["std"], 4) if stat["std"] else None,
            "pixel_count": stat["count"],
        })

    logger.info(f"Aggregated NDVI for {len(records)} wards")
    return records


def aggregate_ndvi_from_gee(ward_geojson_path: str, year: int,
                            month: int) -> list[dict]:
    """
    Alternative: aggregate NDVI directly in GEE (no raster download needed).
    Uses ee.Image.reduceRegions for server-side zonal stats.

    Args:
        ward_geojson_path: Path to ward boundaries GeoJSON
        year: Year
        month: Month

    Returns:
        List of dicts with ward-level NDVI statistics
    """
    import ee

    # Load ward boundaries as ee.FeatureCollection
    with open(ward_geojson_path) as f:
        geojson = json.load(f)

    wards_fc = ee.FeatureCollection(geojson["features"])

    # Compute NDVI
    ndvi = compute_ndvi_composite(year, month)

    # Reduce to ward regions
    results = ndvi.reduceRegions(
        collection=wards_fc,
        reducer=ee.Reducer.mean()
            .combine(ee.Reducer.median(), sharedInputs=True)
            .combine(ee.Reducer.minMax(), sharedInputs=True)
            .combine(ee.Reducer.stdDev(), sharedInputs=True)
            .combine(ee.Reducer.count(), sharedInputs=True),
        scale=EXPORT_SCALE,
    ).getInfo()

    records = []
    for feature in results["features"]:
        props = feature["properties"]
        records.append({
            "ward_id": props.get("ward_id"),
            "ward_name": props.get("ward_name", props.get("name", "")),
            "year": year,
            "month": month,
            "mean_ndvi": round(props.get("mean", 0), 4),
            "median_ndvi": round(props.get("median", 0), 4),
            "min_ndvi": round(props.get("min", 0), 4),
            "max_ndvi": round(props.get("max", 0), 4),
            "std_ndvi": round(props.get("stdDev", 0), 4),
            "pixel_count": int(props.get("count", 0)),
        })

    logger.info(f"GEE-aggregated NDVI for {len(records)} wards")
    return records


def get_prepare_rows(records: list[dict]) -> tuple[list[str], list[tuple]]:
    """
    Prepare NDVI records for bulk DB insertion.

    Returns:
        Tuple of (column_names, row_tuples)
    """
    columns = [
        "ward_id", "ward_name", "year", "month",
        "mean_ndvi", "median_ndvi", "min_ndvi", "max_ndvi",
        "std_ndvi", "pixel_count"
    ]
    rows = [
        (
            r["ward_id"], r["ward_name"], r["year"], r["month"],
            r["mean_ndvi"], r["median_ndvi"], r["min_ndvi"], r["max_ndvi"],
            r["std_ndvi"], r["pixel_count"]
        )
        for r in records
    ]
    return columns, rows
