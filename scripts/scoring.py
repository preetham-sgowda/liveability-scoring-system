import os
import sys
import pandas as pd
import numpy as np
from scipy.stats import pearsonr
from sklearn.preprocessing import MinMaxScaler
from scripts.db_utils import get_db_connection, upsert_rows
import logging

logger = logging.getLogger(__name__)

# PRD Weights
WEIGHTS = {
    "safety": 0.25,
    "environment": 0.20,
    "civic": 0.20,
    "connectivity": 0.15,
    "socioeconomic": 0.10,
    "infrastructure": 0.10
}

def load_ward_features():
    """Load latest enriched ward features."""
    # We load from marts.mart_ward_features if enriched, or fallback to CSV
    try:
        with get_db_connection() as conn:
            return pd.read_sql("SELECT * FROM marts.mart_ward_features", conn)
    except Exception as e:
        logger.warning(f"DB load failed: {e}. Trying CSV fallback...")
        csv_path = os.path.join("data", "processed", "ward_features_enriched.csv")
        if os.path.exists(csv_path):
            return pd.read_csv(csv_path)
        logger.error("No data found")
        return pd.DataFrame()

def calculate_scores(df):
    """Normalize features and calculate weighted composite score."""
    scaler = MinMaxScaler(feature_range=(0, 100))
    
    # Check if transit_walkability exists, if not calculate it
    if "transit_walkability" not in df.columns:
        if "bus_stops_count" in df.columns and "metro_proximity_km" in df.columns:
            bus_norm = scaler.fit_transform(df[["bus_stops_count"]].fillna(0)).flatten()
            metro_inv = 1.0 / (df["metro_proximity_km"].fillna(99.0) + 0.1)
            metro_norm = scaler.fit_transform(metro_inv.values.reshape(-1, 1)).flatten()
            df["transit_walkability"] = bus_norm * 0.5 + metro_norm * 0.5
        else:
            df["transit_walkability"] = 0
            
    if "complaint_backlog_pct" not in df.columns:
        if "pending_complaints" in df.columns and "total_complaints" in df.columns:
            df["complaint_backlog_pct"] = np.where(df["total_complaints"] > 0, df["pending_complaints"] / df["total_complaints"], 0.0)
        else:
            df["complaint_backlog_pct"] = 0.0

    # Dimension feature mappings
    # Note: median_price_sqft is used for VALIDATION ONLY, not for scoring!
    dimensions = {
        "safety": {"cols": ["crime_rate_per_1000"], "invert": True},
        "environment": {"cols": ["avg_aqi", "avg_ndvi"], "invert_cols": ["avg_aqi"]},
        "civic": {"cols": ["resolution_rate", "complaint_backlog_pct"], "invert_cols": ["complaint_backlog_pct"]},
        "connectivity": {"cols": ["transit_walkability"], "invert": False},
        "socioeconomic": {"cols": ["literacy_rate"], "invert": False},
        "infrastructure": {"cols": ["population_density"], "invert_cols": ["population_density"]}
    }
    
    scores = df[["ward_id", "ward_name", "city", "year"]].copy()
    
    for dim, config in dimensions.items():
        cols = config["cols"]
        valid_cols = [c for c in cols if c in df.columns]
        
        if valid_cols:
            # Normalize each column first
            norm_vals = []
            for c in valid_cols:
                # Fill missing with median
                col_data = df[c].fillna(df[c].median()).values.reshape(-1, 1)
                norm_data = scaler.fit_transform(col_data).flatten()
                
                # Invert if needed (e.g., higher crime = worse)
                if config.get("invert", False) or c in config.get("invert_cols", []):
                    norm_data = 100 - norm_data
                
                norm_vals.append(norm_data)
                
            # Average the normalized components
            dim_score = np.mean(norm_vals, axis=0)
            scores[f"{dim}_score"] = dim_score
        else:
            scores[f"{dim}_score"] = 0
            
    # Composite Score
    scores["composite_score"] = (
        scores["safety_score"] * WEIGHTS["safety"] +
        scores["environment_score"] * WEIGHTS["environment"] +
        scores["civic_score"] * WEIGHTS["civic"] +
        scores["connectivity_score"] * WEIGHTS["connectivity"] +
        scores["socioeconomic_score"] * WEIGHTS["socioeconomic"] +
        scores["infrastructure_score"] * WEIGHTS["infrastructure"]
    )
    
    # Validation vs Price
    if "median_price_sqft" in df.columns:
        valid_idx = df["median_price_sqft"].notna() & scores["composite_score"].notna()
        if valid_idx.sum() > 2:
            r, p = pearsonr(scores.loc[valid_idx, "composite_score"], df.loc[valid_idx, "median_price_sqft"])
            logger.info(f"Validation: Pearson r(composite_score, price) = {r:.3f} (p={p:.3f})")
            if r < 0.5:
                logger.warning("Target correlation r > 0.5 not met!")
            else:
                logger.info("Target correlation r > 0.5 achieved!")

    return scores

def save_scores(scores_df):
    """Save processed scores to marts.liveability_scores."""
    # To keep compatibility with original 006 schema, we map our new names to old ones
    # safety_score -> safety_score
    # environment_score -> aqi_score
    # civic_score -> civic_score
    # connectivity_score -> transit_score
    # socioeconomic_score -> affordability_score
    # infrastructure_score -> green_score
    
    db_scores = scores_df.rename(columns={
        "environment_score": "aqi_score",
        "connectivity_score": "transit_score",
        "socioeconomic_score": "affordability_score",
        "infrastructure_score": "green_score"
    })
    
    cols = db_scores.columns.tolist()
    db_scores['month'] = 3 
    
    # Reorder columns to match schema requirements
    save_cols = ["ward_id", "ward_name", "city", "year", "month", 
                 "composite_score", "safety_score", "aqi_score", 
                 "civic_score", "transit_score", "green_score", "affordability_score"]
                 
    save_cols = [c for c in save_cols if c in db_scores.columns]
    
    rows = [tuple(x) for x in db_scores[save_cols].to_numpy()]
    
    try:
        upsert_rows(
            table="marts.liveability_scores",
            columns=save_cols,
            rows=rows,
            conflict_columns=["ward_id", "year", "month"],
            update_columns=["composite_score", "safety_score", "aqi_score", "civic_score", "transit_score", "green_score", "affordability_score"]
        )
        logger.info(f"Saved {len(rows)} scores to database")
    except Exception as e:
        logger.warning(f"Could not save to DB: {e}. Saving to CSV instead.")
        os.makedirs(os.path.join("data", "processed"), exist_ok=True)
        db_scores.to_csv(os.path.join("data", "processed", "liveability_scores.csv"), index=False)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = load_ward_features()
    if not df.empty:
        scores = calculate_scores(df)
        save_scores(scores)
        print("Scoring calculation complete.")
    else:
        print("No features to score.")
