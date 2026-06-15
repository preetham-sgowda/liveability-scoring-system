"""
ml/shap_explainability.py — SHAP explainer for XGBoost decline predictor

Calculates global feature importance and top-3 per-ward SHAP drivers.
Generates waterfall charts for Streamlit dashboard.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
import joblib
import shap
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "outputs")
PROCESSED_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "processed")

def load_data():
    feat_path = os.path.join(PROCESSED_DIR, "ward_features_enriched.csv")
    if os.path.exists(feat_path):
        return pd.read_csv(feat_path)
        
    try:
        from scripts.db_utils import get_db_connection
        with get_db_connection() as conn:
            return pd.read_sql("SELECT * FROM marts.mart_ward_features", conn)
    except Exception as e:
        logger.warning(f"DB load failed: {e}")
        
    return pd.DataFrame()

def run_explainability():
    logger.info("Starting SHAP Explainability...")
    model_path = os.path.join(MODELS_DIR, "decline_predictor_v1.joblib")
    
    if not os.path.exists(model_path):
        logger.error(f"Model not found at {model_path}. Run decline_predictor.py first.")
        return
        
    data = joblib.load(model_path)
    model = data["model"]
    features = data["features"]
    
    df = load_data()
    if df.empty:
        logger.error("No data found!")
        return
        
    # We only care about latest year
    latest_year = df['year'].max()
    df_latest = df[df['year'] == latest_year].copy()
    
    X = df_latest[features].fillna(df_latest[features].median())
    
    logger.info("Computing SHAP values...")
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Global Summary Plot
    plt.figure(figsize=(10, 8))
    shap.summary_plot(shap_values, X, show=False)
    plt.savefig(os.path.join(OUTPUT_DIR, "shap_summary.png"), bbox_inches='tight')
    plt.close()
    logger.info("Saved SHAP summary plot")
    
    # 2. Per-ward top 3 drivers
    drivers_list = []
    
    for i, ward_id in enumerate(df_latest['ward_id']):
        # shap_values[i] contains SHAP value for each feature
        ward_shaps = shap_values[i]
        
        # We want absolute impact to find top 3
        abs_shaps = np.abs(ward_shaps)
        top_3_idx = np.argsort(abs_shaps)[-3:][::-1] # indices of top 3
        
        drivers = {
            'ward_id': ward_id,
            'year': latest_year
        }
        
        for rank, idx in enumerate(top_3_idx):
            drivers[f'driver_{rank+1}_feature'] = features[idx]
            drivers[f'driver_{rank+1}_value'] = float(ward_shaps[idx])
            
        drivers_list.append(drivers)
        
    drivers_df = pd.DataFrame(drivers_list)
    
    # Save to CSV
    drivers_df.to_csv(os.path.join(PROCESSED_DIR, "ward_shap_drivers.csv"), index=False)
    
    # Save to DB
    try:
        from scripts.db_utils import upsert_rows
        cols = ['ward_id', 'year', 'driver_1_feature', 'driver_1_value', 
                'driver_2_feature', 'driver_2_value', 'driver_3_feature', 'driver_3_value']
        rows = [tuple(x) for x in drivers_df[cols].to_numpy()]
        upsert_rows(
            table="marts.ward_shap_drivers",
            columns=cols,
            rows=rows,
            conflict_columns=["ward_id", "year"],
            update_columns=[c for c in cols if c not in ["ward_id", "year"]]
        )
        logger.info("Saved SHAP drivers to DB: marts.ward_shap_drivers")
    except Exception as e:
        logger.warning(f"Failed to save to DB: {e}")

def generate_waterfall(ward_id, year):
    """
    Generate waterfall chart for a specific ward-year.
    Returns the matplotlib figure for Streamlit to render.
    """
    model_path = os.path.join(MODELS_DIR, "decline_predictor_v1.joblib")
    if not os.path.exists(model_path):
        return None
        
    data = joblib.load(model_path)
    model = data["model"]
    features = data["features"]
    
    df = load_data()
    df_ward = df[(df['ward_id'] == ward_id) & (df['year'] == year)]
    
    if df_ward.empty:
        return None
        
    X = df_ward[features].fillna(df[features].median())
    
    explainer = shap.TreeExplainer(model)
    shap_values = explainer(X) # generates Explanation object needed for waterfall
    
    fig = plt.figure(figsize=(10, 6))
    shap.plots.waterfall(shap_values[0], show=False)
    return fig

if __name__ == "__main__":
    run_explainability()
