import pandas as pd
import geopandas as gpd
from sklearn.preprocessing import MinMaxScaler
from scripts.db_utils import get_db_connection, bulk_insert, upsert_rows
import logging

logger = logging.getLogger(__name__)

# Weights
WEIGHTS = {
    "safety": 0.20,
    "aqi": 0.20,
    "civic": 0.15,
    "transit": 0.20,
    "green": 0.15,
    "affordability": 0.10
}

def load_ward_features():
    """Load latest ward features from mart_ward_features or aggregate them."""
    # For now, we'll assume dbt or a SQL view pre-aggregates these into mart_ward_features.
    # If not, we'd do spatial joins here between raw tables and ward_boundaries.
    with get_db_connection() as conn:
        return pd.read_sql("SELECT * FROM marts.mart_ward_features", conn)

def calculate_scores(df):
    """Normalize features and calculate weighted composite score."""
    scaler = MinMaxScaler(feature_range=(0, 100))
    
    # Invert some scores (higher crime = lower score, higher price = lower affordability)
    # Crime: higher count is worse.
    # AQI: higher value is worse.
    # Property Price: higher sqft price is worse for affordability.
    
    # Map raw features to dimensions
    dimensions = {
        "safety": ["crime_rate_per_1000"],
        "aqi": ["avg_aqi"],
        "civic": ["resolution_rate"],
        "transit": ["transit_score"],
        "green": ["avg_ndvi"],
        "affordability": ["median_price_sqft"]
    }
    
    scores = df[["ward_id", "ward_name", "city", "year"]].copy()
    
    for dim, cols in dimensions.items():
        if all(c in df.columns for c in cols):
            # Simple average if multiple columns
            val = df[cols].mean(axis=1)
            # Normalization
            norm_val = scaler.fit_transform(val.values.reshape(-1, 1)).flatten()
            
            # Inversion logic
            if dim in ["safety", "aqi", "affordability"]:
                norm_val = 100 - norm_val
                
            scores[f"{dim}_score"] = norm_val
        else:
            scores[f"{dim}_score"] = 0
            
    # Composite Score
    scores["composite_score"] = (
        scores["safety_score"] * WEIGHTS["safety"] +
        scores["aqi_score"] * WEIGHTS["aqi"] +
        scores["civic_score"] * WEIGHTS["civic"] +
        scores["transit_score"] * WEIGHTS["transit"] +
        scores["green_score"] * WEIGHTS["green"] +
        scores["affordability_score"] * WEIGHTS["affordability"]
    )
    
    return scores

def save_scores(scores_df):
    """Save processed scores to marts.liveability_scores."""
    cols = scores_df.columns.tolist()
    # Add dummy month for now
    scores_df['month'] = 3 
    rows = [tuple(x) for x in scores_df.to_numpy()]
    
    upsert_rows(
        table="marts.liveability_scores",
        columns=cols + ['month'],
        rows=rows,
        conflict_columns=["ward_id", "year", "month"],
        update_columns=["composite_score", "safety_score", "aqi_score", "civic_score", "transit_score", "green_score", "affordability_score"]
    )

if __name__ == "__main__":
    df = load_ward_features()
    if not df.empty:
        scores = calculate_scores(df)
        save_scores(scores)
        print("Scoring calculation complete.")
    else:
        print("No features to score.")
