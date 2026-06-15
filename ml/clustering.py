"""
ml/clustering.py — KMeans clustering for Ward Typology

Clusters wards into 6 archetypes based on key liveability features.
Uses KMeans and UMAP for 2D projection.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import umap

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

FEATURES_FOR_CLUSTERING = [
    'crime_rate_per_1000', 'avg_aqi', 'resolution_rate',
    'transit_walkability', 'avg_ndvi', 'population_density', 'literacy_rate'
]

CLUSTER_NAMES = {
    0: "Urban Core",
    1: "Established Residential",
    2: "Green Suburbs",
    3: "Distressed",
    4: "Emerging",
    5: "Industrial / Transit Hub",
    6: "High-Density Periphery",
    7: "Developing Suburbs"
}

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "outputs")
PROCESSED_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "processed")

def load_data():
    csv_path = os.path.join(PROCESSED_DIR, "ward_features_enriched.csv")
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    
    # Fallback to DB
    from scripts.db_utils import get_db_connection
    with get_db_connection() as conn:
        return pd.read_sql("SELECT * FROM marts.mart_ward_features", conn)

def run_clustering():
    logger.info("Starting KMeans Clustering for Ward Typology...")
    df = load_data()
    
    if df.empty:
        logger.error("No data found!")
        return
        
    # We cluster on the latest year available
    latest_year = df['year'].max()
    df_latest = df[df['year'] == latest_year].copy()
    logger.info(f"Using {len(df_latest)} wards from year {latest_year}")
    
    # Ensure all features exist
    missing = [f for f in FEATURES_FOR_CLUSTERING if f not in df_latest.columns]
    if missing:
        logger.error(f"Missing features: {missing}")
        return
        
    # Extract features and handle NaNs
    X_raw = df_latest[FEATURES_FOR_CLUSTERING].fillna(df_latest[FEATURES_FOR_CLUSTERING].median())
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_raw)
    
    # Find best K
    best_k = 6 # Default fallback
    best_score = -1
    
    for k in range(3, 9):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X_scaled)
        score = silhouette_score(X_scaled, labels)
        logger.info(f"k={k}, silhouette_score={score:.4f}")
        if score > best_score:
            best_score = score
            best_k = k
            
    logger.info(f"Selected best k={best_k} with silhouette score {best_score:.4f}")
    
    if best_score < 0.35:
        logger.warning(f"Target silhouette score > 0.35 not met! (Got {best_score:.4f})")
    
    # Final Model
    kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    df_latest['cluster_id'] = kmeans.fit_predict(X_scaled)
    df_latest['cluster_label'] = df_latest['cluster_id'].map(lambda x: CLUSTER_NAMES.get(x, f"Cluster {x}"))
    
    # Calculate profiles
    profiles = df_latest.groupby('cluster_label')[FEATURES_FOR_CLUSTERING].mean()
    logger.info("Cluster Profiles:\n" + str(profiles))
    
    # UMAP Projection
    logger.info("Computing UMAP 2D projection...")
    reducer = umap.UMAP(n_components=2, random_state=42)
    embedding = reducer.fit_transform(X_scaled)
    
    df_latest['umap_x'] = embedding[:, 0]
    df_latest['umap_y'] = embedding[:, 1]
    
    # Save outputs
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    umap_df = df_latest[['ward_id', 'ward_name', 'cluster_id', 'cluster_label', 'umap_x', 'umap_y']]
    umap_df.to_csv(os.path.join(OUTPUT_DIR, "umap_projection.csv"), index=False)
    
    # Save full clusters
    cluster_df = df_latest[['ward_id', 'year', 'cluster_id', 'cluster_label']]
    cluster_df.to_csv(os.path.join(PROCESSED_DIR, "ward_clusters.csv"), index=False)
    
    # Save to DB
    try:
        from scripts.db_utils import upsert_rows
        rows = [tuple(x) for x in cluster_df.to_numpy()]
        upsert_rows(
            table="marts.ward_clusters",
            columns=["ward_id", "year", "cluster_id", "cluster_label"],
            rows=rows,
            conflict_columns=["ward_id", "year"],
            update_columns=["cluster_id", "cluster_label"]
        )
        logger.info("Saved clusters to DB: marts.ward_clusters")
    except Exception as e:
        logger.warning(f"Failed to save to DB: {e}")

if __name__ == "__main__":
    run_clustering()
