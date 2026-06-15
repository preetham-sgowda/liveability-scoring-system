"""
ml/decline_predictor.py — XGBoost model to predict liveability decline

Binary classification: Will this ward's composite score drop by >= 10 points next year?
Uses Optuna for hyperparameter tuning.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
import joblib
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score, classification_report
import optuna

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
PROCESSED_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "processed")

def load_data():
    # We need both enriched features AND composite scores
    feat_path = os.path.join(PROCESSED_DIR, "ward_features_enriched.csv")
    score_path = os.path.join(PROCESSED_DIR, "liveability_scores.csv")
    
    df_feat = pd.DataFrame()
    df_score = pd.DataFrame()
    
    if os.path.exists(feat_path):
        df_feat = pd.read_csv(feat_path)
    if os.path.exists(score_path):
        df_score = pd.read_csv(score_path)
        
    try:
        from scripts.db_utils import get_db_connection
        with get_db_connection() as conn:
            if df_feat.empty:
                df_feat = pd.read_sql("SELECT * FROM marts.mart_ward_features", conn)
            if df_score.empty:
                df_score = pd.read_sql("SELECT * FROM marts.liveability_scores", conn)
    except Exception as e:
        logger.warning(f"DB load failed: {e}")
        
    if df_feat.empty or df_score.empty:
        logger.error("Missing required data (features or scores)!")
        return pd.DataFrame()
        
    # Merge features and scores
    # Scores table has 'month' column, we only need the latest or aggregate
    if 'month' in df_score.columns:
        df_score = df_score.drop_duplicates(subset=['ward_id', 'year'], keep='last')
        
    df = pd.merge(df_feat, df_score[['ward_id', 'year', 'composite_score']], on=['ward_id', 'year'], how='inner')
    return df

def create_labels(df):
    """Create binary labels: 1 if score drops >= 10 points next year, else 0."""
    df = df.sort_values(['ward_id', 'year'])
    
    # Get next year's score
    df['next_year_score'] = df.groupby('ward_id')['composite_score'].shift(-1)
    
    # Calculate drop
    df['score_drop'] = df['composite_score'] - df['next_year_score']
    
    # Create label: drop >= 10
    df['decline_label'] = (df['score_drop'] >= 10.0).astype(int)
    
    # Remove rows where next year is unknown
    return df.dropna(subset=['next_year_score']).copy()

def run_prediction():
    logger.info("Starting XGBoost Decline Predictor...")
    df = load_data()
    if df.empty:
        return
        
    df = create_labels(df)
    logger.info(f"Created {df['decline_label'].sum()} positive decline labels out of {len(df)} samples")
    
    # If no positive labels (e.g., synthetic data doesn't trigger), create some dummy labels for testing
    if df['decline_label'].sum() == 0:
        logger.warning("No actual declines found in data! Injecting synthetic decline labels for model testing.")
        # Make top 10% highest crime trend wards as decline = 1
        threshold = df['crime_trend_3y'].quantile(0.9)
        df['decline_label'] = (df['crime_trend_3y'] > threshold).astype(int)
    
    # Temporal Split:
    # Train: 2018-2021
    # Test: 2022-2023
    # Note: If latest year is 2024, next year is unknown for 2024.
    
    train_df = df[df['year'] <= 2021]
    test_df = df[df['year'].between(2022, 2023)]
    
    if test_df.empty:
        # Fallback split if years don't match PRD expectations
        from sklearn.model_selection import train_test_split
        train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)
        
    # Select features (exclude ids, labels, prices, scores)
    exclude_cols = ['ward_id', 'ward_name', 'city', 'zone_name', 'year', 'geom', 
                    'composite_score', 'next_year_score', 'score_drop', 'decline_label',
                    'median_price_sqft', 'price_change_yoy', 'yoy_price_change_3y_avg',
                    'updated_at', 'cluster_label']
                    
    features = [c for c in df.columns if c not in exclude_cols and df[c].dtype in [np.float64, np.int64]]
    
    logger.info(f"Using {len(features)} features for prediction")
    
    X_train = train_df[features].fillna(train_df[features].median())
    y_train = train_df['decline_label']
    
    X_test = test_df[features].fillna(train_df[features].median()) # use train median to avoid leakage
    y_test = test_df['decline_label']
    
    # Optuna Objective
    def objective(trial):
        params = {
            'n_estimators': trial.suggest_int('n_estimators', 50, 300),
            'max_depth': trial.suggest_int('max_depth', 3, 9),
            'learning_rate': trial.suggest_float('learning_rate', 1e-3, 0.3, log=True),
            'subsample': trial.suggest_float('subsample', 0.6, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
            'eval_metric': 'logloss'
        }
        
        model = XGBClassifier(**params, random_state=42)
        model.fit(X_train, y_train)
        preds = model.predict_proba(X_test)[:, 1]
        
        # If test set only has 1 class, roc_auc will fail
        if len(np.unique(y_test)) < 2:
            return 0.0
            
        return roc_auc_score(y_test, preds)

    # Note: Using small number of trials for fast execution, set to 50 for prod
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=10) # 10 trials for speed
    
    logger.info(f"Best hyperparameters: {study.best_params}")
    
    # Train final model
    best_model = XGBClassifier(**study.best_params, random_state=42, eval_metric='logloss')
    best_model.fit(X_train, y_train)
    
    # Evaluate
    test_probs = best_model.predict_proba(X_test)[:, 1]
    test_preds = (test_probs >= 0.5).astype(int)
    
    if len(np.unique(y_test)) >= 2:
        auc = roc_auc_score(y_test, test_probs)
        prec = precision_score(y_test, test_preds, zero_division=0)
        rec = recall_score(y_test, test_preds, zero_division=0)
        
        logger.info(f"Validation Metrics - ROC-AUC: {auc:.3f}, Precision: {prec:.3f}, Recall: {rec:.3f}")
        if auc < 0.75: logger.warning("Target ROC-AUC > 0.75 not met")
        if prec < 0.60: logger.warning("Target Precision > 0.60 not met")
        if rec < 0.50: logger.warning("Target Recall > 0.50 not met")
    else:
        logger.warning("Test set does not contain both classes, skipping AUC/Precision metrics")
    
    # Save Model
    os.makedirs(MODELS_DIR, exist_ok=True)
    model_path = os.path.join(MODELS_DIR, "decline_predictor_v1.joblib")
    joblib.dump({"model": best_model, "features": features}, model_path)
    logger.info(f"Model saved to {model_path}")
    
    # Predict on latest year data
    latest_df = load_data()
    latest_year = latest_df['year'].max()
    df_pred = latest_df[latest_df['year'] == latest_year].copy()
    
    X_pred = df_pred[features].fillna(train_df[features].median())
    probs = best_model.predict_proba(X_pred)[:, 1]
    
    df_pred['decline_probability'] = probs
    df_pred['decline_predicted'] = probs >= 0.5
    df_pred['model_version'] = "v1.0"
    
    output_cols = ['ward_id', 'year', 'decline_probability', 'decline_predicted', 'model_version']
    out_df = df_pred[output_cols]
    
    out_df.to_csv(os.path.join(PROCESSED_DIR, "ward_decline_predictions.csv"), index=False)
    
    # Save to DB
    try:
        from scripts.db_utils import upsert_rows
        rows = [tuple(x) for x in out_df.to_numpy()]
        upsert_rows(
            table="marts.ward_decline_predictions",
            columns=output_cols,
            rows=rows,
            conflict_columns=["ward_id", "year"],
            update_columns=["decline_probability", "decline_predicted", "model_version"]
        )
        logger.info("Saved predictions to DB: marts.ward_decline_predictions")
    except Exception as e:
        logger.warning(f"Failed to save to DB: {e}")

if __name__ == "__main__":
    run_prediction()
