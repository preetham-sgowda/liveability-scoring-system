"""
ml/feature_engineering.py — Feature Engineering for Liveability Scoring System

Loads mart_ward_features, applies KNN imputation, computes derived features
(trends, backlog pct, transit walkability), and saves enriched dataset.

Usage:
    python -m ml.feature_engineering
"""

import os
import sys
import logging
import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer
from sklearn.preprocessing import MinMaxScaler

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── Feature columns to impute ──
NUMERIC_FEATURES = [
    "crime_rate_per_1000", "total_ipc_offenses", "murder_count", "theft_count",
    "robbery_count", "assault_count", "kidnapping_count", "burglary_count",
    "total_complaints", "pending_complaints", "resolved_complaints",
    "resolution_rate", "avg_resolution_days",
    "avg_pm25", "avg_pm10", "avg_no2", "avg_so2", "avg_aqi", "aqi_good_days_pct",
    "population", "population_density", "literacy_rate",
    "avg_household_size", "total_households",
    "bus_stops_count", "metro_proximity_km", "avg_route_frequency", "total_routes",
    "avg_ndvi", "ndvi_change_yoy", "green_cover_pct",
    "median_price_sqft", "price_change_yoy",
]

# Output paths
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "data", "processed")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "ward_features_enriched.csv")


def load_ward_features() -> pd.DataFrame:
    """Load mart_ward_features from PostgreSQL, falling back to CSV."""
    try:
        from scripts.db_utils import get_db_connection
        with get_db_connection() as conn:
            df = pd.read_sql(
                "SELECT * FROM marts.mart_ward_features ORDER BY ward_id, year",
                conn,
            )
        logger.info(f"Loaded {len(df)} rows from PostgreSQL")
        return df
    except Exception as e:
        logger.warning(f"DB load failed ({e}), trying CSV fallback...")
        csv_path = OUTPUT_CSV
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} rows from CSV fallback")
            return df
        logger.error("No data source available")
        return pd.DataFrame()


def forward_fill_census(df: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill census data across years per ward (census is 2011 only)."""
    census_cols = [
        "population", "population_density", "literacy_rate",
        "avg_household_size", "total_households",
    ]
    existing_cols = [c for c in census_cols if c in df.columns]
    if not existing_cols:
        return df

    df = df.sort_values(["ward_id", "year"])
    df[existing_cols] = df.groupby("ward_id")[existing_cols].ffill()
    # Also backward-fill in case earliest years have data
    df[existing_cols] = df.groupby("ward_id")[existing_cols].bfill()
    logger.info(f"Forward-filled census columns: {existing_cols}")
    return df


def knn_impute(df: pd.DataFrame, k: int = 5) -> pd.DataFrame:
    """KNN impute features with <30% missingness; drop features with >50%."""
    feature_cols = [c for c in NUMERIC_FEATURES if c in df.columns]
    if not feature_cols:
        logger.warning("No numeric features found for imputation")
        return df

    missingness = df[feature_cols].isnull().mean()

    # Drop features with >50% missingness
    drop_cols = missingness[missingness > 0.50].index.tolist()
    if drop_cols:
        logger.warning(f"Dropping high-missingness features (>50%): {drop_cols}")
        # Don't drop, just flag them
        for col in drop_cols:
            df[f"{col}_flagged_missing"] = df[col].isnull().astype(int)

    # Impute features with <30% missingness
    impute_cols = missingness[(missingness > 0) & (missingness <= 0.30)].index.tolist()
    if impute_cols:
        logger.info(f"KNN imputing (k={k}) columns: {impute_cols}")
        imputer = KNNImputer(n_neighbors=k)
        df[impute_cols] = imputer.fit_transform(df[impute_cols])

    return df


def compute_3y_trend(group: pd.DataFrame, col: str) -> float:
    """Compute 3-year linear trend (slope) for a column within a ward group."""
    recent = group.nlargest(3, "year")
    if len(recent) < 2 or recent[col].isnull().all():
        return np.nan
    x = recent["year"].values.astype(float)
    y = recent[col].values.astype(float)
    mask = ~np.isnan(y)
    if mask.sum() < 2:
        return np.nan
    # Linear regression slope: β = Σ(x-x̄)(y-ȳ) / Σ(x-x̄)²
    x_clean, y_clean = x[mask], y[mask]
    x_mean, y_mean = x_clean.mean(), y_clean.mean()
    denom = ((x_clean - x_mean) ** 2).sum()
    if denom == 0:
        return 0.0
    return float(((x_clean - x_mean) * (y_clean - y_mean)).sum() / denom)


def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute all derived / engineered features."""
    df = df.sort_values(["ward_id", "year"]).copy()

    # ── 3-year trends ──
    trend_features = {
        "crime_trend_3y": "crime_rate_per_1000",
        "aqi_trend_3y": "avg_aqi",
        "ndvi_trend_3y": "avg_ndvi",
    }
    for new_col, source_col in trend_features.items():
        if source_col in df.columns:
            trends = df.groupby("ward_id").apply(
                lambda g: pd.Series(
                    {new_col: compute_3y_trend(g, source_col)},
                    index=[new_col]
                )
            ).reset_index()
            # Merge trend back — one value per ward, apply to latest year rows
            df = df.merge(trends[["ward_id", new_col]], on="ward_id", how="left",
                          suffixes=("_old", ""))
            if f"{new_col}_old" in df.columns:
                df.drop(columns=[f"{new_col}_old"], inplace=True)
        else:
            df[new_col] = np.nan

    # ── Complaint backlog percentage ──
    if "pending_complaints" in df.columns and "total_complaints" in df.columns:
        df["complaint_backlog_pct"] = np.where(
            df["total_complaints"] > 0,
            df["pending_complaints"] / df["total_complaints"],
            0.0,
        )
    else:
        df["complaint_backlog_pct"] = 0.0

    # ── Transit walkability (composite of bus stops + inverse metro proximity) ──
    if "bus_stops_count" in df.columns and "metro_proximity_km" in df.columns:
        scaler = MinMaxScaler(feature_range=(0, 100))
        bus_norm = scaler.fit_transform(
            df[["bus_stops_count"]].fillna(0)
        ).flatten()
        # Inverse metro proximity: closer = higher score
        metro_inv = 1.0 / (df["metro_proximity_km"].fillna(99.0) + 0.1)
        metro_norm = scaler.fit_transform(metro_inv.values.reshape(-1, 1)).flatten()
        df["transit_walkability"] = (bus_norm * 0.5 + metro_norm * 0.5).round(2)
    else:
        df["transit_walkability"] = np.nan

    # ── 3-year rolling average of YoY price change ──
    if "price_change_yoy" in df.columns:
        df["yoy_price_change_3y_avg"] = (
            df.sort_values(["ward_id", "year"])
            .groupby("ward_id")["price_change_yoy"]
            .transform(lambda x: x.rolling(3, min_periods=1).mean())
        )
    else:
        df["yoy_price_change_3y_avg"] = np.nan

    logger.info("Derived features computed: crime_trend_3y, aqi_trend_3y, "
                "ndvi_trend_3y, complaint_backlog_pct, transit_walkability, "
                "yoy_price_change_3y_avg")
    return df


def save_enriched(df: pd.DataFrame):
    """Save enriched features to CSV and optionally to DB."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Drop geometry column for CSV (it's WKB binary)
    save_df = df.drop(columns=["geom"], errors="ignore")
    save_df.to_csv(OUTPUT_CSV, index=False)
    logger.info(f"Saved enriched features to {OUTPUT_CSV} ({len(save_df)} rows)")

    # Try DB save
    try:
        from scripts.db_utils import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Upsert derived columns back to mart_ward_features
                for _, row in df.iterrows():
                    cur.execute("""
                        UPDATE marts.mart_ward_features SET
                            crime_trend_3y = %s,
                            aqi_trend_3y = %s,
                            ndvi_trend_3y = %s,
                            complaint_backlog_pct = %s,
                            transit_walkability = %s,
                            yoy_price_change_3y_avg = %s
                        WHERE ward_id = %s AND year = %s
                    """, (
                        row.get("crime_trend_3y"),
                        row.get("aqi_trend_3y"),
                        row.get("ndvi_trend_3y"),
                        row.get("complaint_backlog_pct"),
                        row.get("transit_walkability"),
                        row.get("yoy_price_change_3y_avg"),
                        row["ward_id"], row["year"],
                    ))
            conn.commit()
        logger.info("Updated derived columns in marts.mart_ward_features")
    except Exception as e:
        logger.warning(f"DB upsert skipped: {e}")


def run():
    """Main feature engineering pipeline."""
    logger.info("=" * 60)
    logger.info("FEATURE ENGINEERING PIPELINE — START")
    logger.info("=" * 60)

    # 1. Load data
    df = load_ward_features()
    if df.empty:
        logger.error("No data loaded. Exiting.")
        return None

    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    logger.info(f"Wards: {df['ward_id'].nunique()}, Years: {sorted(df['year'].unique())}")

    # 2. Forward-fill census data
    df = forward_fill_census(df)

    # 3. KNN imputation
    df = knn_impute(df, k=5)

    # 4. Derived features
    df = add_derived_features(df)

    # 5. Save
    save_enriched(df)

    logger.info("=" * 60)
    logger.info("FEATURE ENGINEERING PIPELINE — COMPLETE")
    logger.info("=" * 60)
    return df


if __name__ == "__main__":
    run()
