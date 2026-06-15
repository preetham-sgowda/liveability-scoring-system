import os
import pandas as pd
import numpy as np

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "processed")

def generate_synthetic_data():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Read real wards from GeoJSON to ensure map matching
    geojson_path = os.path.join(os.path.dirname(OUTPUT_DIR), "wards", "bengaluru_wards.geojson")
    import json
    with open(geojson_path, "r", encoding="utf-8") as f:
        geo_data = json.load(f)
        
    wards = []
    for feature in geo_data.get("features", []):
        props = feature.get("properties", {})
        wid = props.get("id") or props.get("WARD_NO")
        wname = props.get("name_en") or props.get("WARD_NAME")
        if wid is not None and wname:
            wards.append((int(wid), str(wname)))
            
    if not wards:
        # Fallback if parsing fails
        wards = [(i, f"Ward {i}") for i in range(1, 199)]
        
    ward_ids = [w[0] for w in wards]
    ward_names = [w[1] for w in wards]
    n_wards = len(wards)
    
    # Base parameters
    years = [2021, 2022, 2023, 2024]
    city = "Bengaluru"
    
    # 1. liveability_scores.csv
    scores_data = []
    features_data = []
    
    # Add a base score for each ward so it varies per ward, then varies over years
    np.random.seed(42)
    ward_base_scores = np.random.uniform(40, 85, n_wards)
    
    for i, w_id in enumerate(ward_ids):
        w_name = ward_names[i]
        base_score = ward_base_scores[i]
        
        for yr in years:
            # Vary by year slightly
            yr_var = np.random.uniform(-5, 5)
            comp = np.clip(base_score + yr_var, 0, 100)
            
            # Sub-scores
            safety = np.clip(comp + np.random.uniform(-10, 10), 0, 100)
            aqi = np.clip(comp + np.random.uniform(-15, 15), 0, 100)
            civic = np.clip(comp + np.random.uniform(-10, 10), 0, 100)
            transit = np.clip(comp + np.random.uniform(-20, 20), 0, 100)
            green = np.clip(comp + np.random.uniform(-10, 10), 0, 100)
            afford = np.clip(comp + np.random.uniform(-15, 15), 0, 100)
            
            scores_data.append([w_id, w_name, city, yr, 3, comp, safety, aqi, civic, transit, green, afford])
            
            # Generate features
            price = np.random.uniform(3000, 15000)
            # Add some correlation: higher score = higher price
            price += comp * 50
            features_data.append([w_id, w_name, city, yr, price, safety * 0.1, aqi * 1.5])

    # Save scores
    scores_df = pd.DataFrame(scores_data, columns=["ward_id", "ward_name", "city", "year", "month", "composite_score", "safety_score", "aqi_score", "civic_score", "transit_score", "green_score", "affordability_score"])
    scores_df.to_csv(os.path.join(OUTPUT_DIR, "liveability_scores.csv"), index=False)
    
    # Save features
    feat_df = pd.DataFrame(features_data, columns=["ward_id", "ward_name", "city", "year", "median_price_sqft", "crime_rate_per_1000", "avg_aqi"])
    feat_df.to_csv(os.path.join(OUTPUT_DIR, "ward_features_enriched.csv"), index=False)
    
    # 2. Clusters (Latest year usually)
    clusters = ["Urban Core", "Green Suburbs", "Distressed", "Developing", "Commercial Hub"]
    cluster_data = []
    for yr in years:
        for w_id in ward_ids:
            c_id = np.random.randint(0, len(clusters))
            cluster_data.append([w_id, c_id, clusters[c_id], yr])
    
    pd.DataFrame(cluster_data, columns=["ward_id", "cluster_id", "cluster_label", "year"]).to_csv(
        os.path.join(OUTPUT_DIR, "ward_clusters.csv"), index=False
    )
    
    # 3. Decline Predictions
    decline_data = []
    for yr in years:
        for w_id in ward_ids:
            prob = np.random.beta(2, 5) # Skewed towards low probability
            decline_data.append([w_id, yr, prob, int(prob > 0.5), "v1.0"])
            
    pd.DataFrame(decline_data, columns=["ward_id", "year", "decline_probability", "decline_predicted", "model_version"]).to_csv(
        os.path.join(OUTPUT_DIR, "ward_decline_predictions.csv"), index=False
    )
    
    # 4. SHAP Drivers
    drivers = ["Crime Rate", "Air Quality", "Transit Access", "Pending Complaints", "Green Cover Loss", "Price Surge"]
    shap_data = []
    for yr in years:
        for w_id in ward_ids:
            d1, d2 = np.random.choice(drivers, 2, replace=False)
            shap_data.append([w_id, yr, d1, d2])
            
    pd.DataFrame(shap_data, columns=["ward_id", "year", "driver_1_feature", "driver_2_feature"]).to_csv(
        os.path.join(OUTPUT_DIR, "ward_shap_drivers.csv"), index=False
    )
    
    # 5. UMAP projections (Optional, but page 8 uses it)
    # The UMAP file expects 'umap_x', 'umap_y', 'cluster_label', 'ward_name'
    # Actually stream_lit just reads "umap_projection.csv" from ml/outputs
    ml_output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml", "outputs")
    os.makedirs(ml_output_dir, exist_ok=True)
    umap_data = []
    for i, w_id in enumerate(ward_ids):
        # We just generate 1 record per ward for simplicity
        c_id = np.random.randint(0, len(clusters))
        umap_data.append([ward_names[i], np.random.randn() * 5, np.random.randn() * 5, clusters[c_id]])
        
    pd.DataFrame(umap_data, columns=["ward_name", "umap_x", "umap_y", "cluster_label"]).to_csv(
        os.path.join(ml_output_dir, "umap_projection.csv"), index=False
    )

    print("Synthetic data successfully generated!")

if __name__ == "__main__":
    generate_synthetic_data()
