"""
app/streamlit_app.py — Streamlit Dashboard for Liveability Scoring System
"""

import os
import json
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
import sys

# Configure page layout
st.set_page_config(page_title="Liveability Scoring System", layout="wide", page_icon="🏙️")

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Paths for CSV fallback
PROCESSED_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "processed")
GEOJSON_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "wards", "bengaluru_wards.geojson")

# Helper to load data
@st.cache_data
def load_data(table_name):
    # Try DB first
    try:
        from scripts.db_utils import get_db_connection
        with get_db_connection() as conn:
            return pd.read_sql(f"SELECT * FROM marts.{table_name}", conn)
    except Exception as e:
        # Fallback to CSV
        csv_map = {
            "liveability_scores": "liveability_scores.csv",
            "mart_ward_features": "ward_features_enriched.csv",
            "ward_clusters": "ward_clusters.csv",
            "ward_decline_predictions": "ward_decline_predictions.csv",
            "ward_shap_drivers": "ward_shap_drivers.csv"
        }
        if table_name in csv_map:
            path = os.path.join(PROCESSED_DIR, csv_map[table_name])
            if os.path.exists(path):
                return pd.read_csv(path)
    return pd.DataFrame()

# Load main datasets
df_scores = load_data("liveability_scores")
df_features = load_data("mart_ward_features")
df_clusters = load_data("ward_clusters")
df_decline = load_data("ward_decline_predictions")
df_shap = load_data("ward_shap_drivers")

# Sidebar navigation
st.sidebar.title("🏙️ Liveability System")
city = st.sidebar.selectbox("Select City", ["Bengaluru", "Mumbai", "Delhi"], index=0)

pages = [
    "1. Landing Map",
    "2. Ward Details",
    "3. Compare Wards",
    "4. Declining Ward Alerts",
    "5. Trends Dashboard",
    "6. Opportunity Wards",
    "7. City Overview",
    "8. Ward Typology",
    "9. Data Explorer",
    "10. Tech Showcase"
]

page = st.sidebar.radio("Navigation", pages)

if df_scores.empty or df_features.empty:
    st.error("Data could not be loaded. Please ensure data pipelines have been run.")
    st.stop()

# Filter for latest year for static pages
latest_year = df_scores['year'].max()
df_scores_latest = df_scores[df_scores['year'] == latest_year]

# ---- 1. Landing Map ----
if page == "1. Landing Map":
    st.title(f"Liveability Map — {city} ({latest_year})")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.subheader("Search Ward")
        ward_names = df_scores_latest['ward_name'].tolist()
        search_ward = st.selectbox("Select a ward to locate", [""] + ward_names)
    
    with col1:
        # Simple folium map
        if os.path.exists(GEOJSON_PATH):
            m = folium.Map(location=[12.9716, 77.5946], zoom_start=11, tiles="CartoDB positron")
            
            # Map score to color
            def get_color(score):
                if score >= 70: return "#2ecc71" # Green
                elif score >= 40: return "#f1c40f" # Yellow
                else: return "#e74c3c" # Red
                
            folium.Choropleth(
                geo_data=GEOJSON_PATH,
                name="Liveability Score",
                data=df_scores_latest,
                columns=["ward_name", "composite_score"],
                key_on="feature.properties.ward_name",
                fill_color="YlGnBu",
                fill_opacity=0.7,
                line_opacity=0.2,
                legend_name="Composite Liveability Score"
            ).add_to(m)
            
            st_folium(m, width="100%", height=600)
        else:
            st.warning(f"GeoJSON not found at {GEOJSON_PATH}")

# ---- 2. Ward Details ----
elif page == "2. Ward Details":
    st.title("Ward Deep Dive")
    
    ward_names = sorted(df_scores['ward_name'].unique())
    selected_ward = st.selectbox("Select Ward", ward_names)
    
    ward_data = df_scores[df_scores['ward_name'] == selected_ward].sort_values('year')
    if not ward_data.empty:
        latest_data = ward_data.iloc[-1]
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Composite Score", f"{latest_data['composite_score']:.1f}/100", 
                   f"{latest_data['composite_score'] - ward_data.iloc[-2]['composite_score']:.1f}" if len(ward_data) > 1 else "0")
        
        st.subheader("Dimension Radar")
        categories = ['Safety', 'AQI', 'Civic', 'Transit', 'Green', 'Affordability']
        values = [latest_data['safety_score'], latest_data['aqi_score'], latest_data['civic_score'], 
                  latest_data['transit_score'], latest_data['green_score'], latest_data['affordability_score']]
        
        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(r=values, theta=categories, fill='toself', name=selected_ward))
        fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 100])), showlegend=False)
        st.plotly_chart(fig)
        
        st.subheader("Historical Trend")
        fig_trend = px.line(ward_data, x="year", y="composite_score", markers=True, title="Composite Score Over Time")
        st.plotly_chart(fig_trend)

# ---- 3. Compare Wards ----
elif page == "3. Compare Wards":
    st.title("Compare Wards")
    
    ward_names = sorted(df_scores['ward_name'].unique())
    col1, col2 = st.columns(2)
    with col1: ward_a = st.selectbox("Ward A", ward_names, index=0)
    with col2: ward_b = st.selectbox("Ward B", ward_names, index=1 if len(ward_names)>1 else 0)
    
    a_data = df_scores_latest[df_scores_latest['ward_name'] == ward_a].iloc[0]
    b_data = df_scores_latest[df_scores_latest['ward_name'] == ward_b].iloc[0]
    
    dims = ['composite_score', 'safety_score', 'aqi_score', 'civic_score', 'transit_score', 'green_score', 'affordability_score']
    
    comp_df = pd.DataFrame({
        'Dimension': dims,
        ward_a: [a_data[d] for d in dims],
        ward_b: [b_data[d] for d in dims]
    })
    
    # Plotly grouped bar
    fig = go.Figure()
    fig.add_trace(go.Bar(x=comp_df['Dimension'], y=comp_df[ward_a], name=ward_a))
    fig.add_trace(go.Bar(x=comp_df['Dimension'], y=comp_df[ward_b], name=ward_b))
    fig.update_layout(barmode='group', title="Dimension Comparison")
    st.plotly_chart(fig)

# ---- 4. Declining Ward Alerts ----
elif page == "4. Declining Ward Alerts":
    st.title("Declining Ward Alerts (ML Predictions)")
    
    if not df_decline.empty and not df_shap.empty:
        latest_decline = df_decline[df_decline['year'] == df_decline['year'].max()]
        
        # Merge with names and SHAP
        alerts = pd.merge(latest_decline, df_scores_latest[['ward_id', 'ward_name']], on='ward_id')
        alerts = pd.merge(alerts, df_shap[['ward_id', 'driver_1_feature', 'driver_2_feature']], on='ward_id')
        
        alerts = alerts.sort_values('decline_probability', ascending=False)
        
        st.subheader("Top At-Risk Wards")
        threshold = st.slider("Risk Threshold", 0.0, 1.0, 0.5)
        filtered = alerts[alerts['decline_probability'] >= threshold]
        
        for _, row in filtered.head(10).iterrows():
            with st.expander(f"⚠️ {row['ward_name']} — Risk: {row['decline_probability']:.1%}"):
                st.progress(row['decline_probability'])
                st.write(f"**Top Risk Drivers:** 1. {row['driver_1_feature']} | 2. {row['driver_2_feature']}")
    else:
        st.warning("Decline prediction data not available.")

# ---- 5. Trends Dashboard ----
elif page == "5. Trends Dashboard":
    st.title("Temporal Trends")
    
    ward_names = sorted(df_scores['ward_name'].unique())
    selected_wards = st.multiselect("Select Wards", ward_names, default=[ward_names[0]])
    
    trend_data = df_scores[df_scores['ward_name'].isin(selected_wards)]
    
    if not trend_data.empty:
        fig = px.line(trend_data, x="year", y="composite_score", color="ward_name", title="Composite Score Trends")
        st.plotly_chart(fig)

# ---- 6. Opportunity Wards ----
elif page == "6. Opportunity Wards":
    st.title("Opportunity Wards (High Score, Low Price)")
    
    if 'median_price_sqft' in df_features.columns:
        latest_feat = df_features[df_features['year'] == latest_year]
        merged = pd.merge(df_scores_latest, latest_feat[['ward_id', 'median_price_sqft']], on='ward_id')
        
        merged = merged[merged['median_price_sqft'] > 0]
        
        fig = px.scatter(merged, x="median_price_sqft", y="composite_score", hover_name="ward_name",
                        title="Liveability vs. Property Price",
                        labels={"median_price_sqft": "Price per Sqft", "composite_score": "Liveability Score"})
        
        # Quadrant lines
        fig.add_hline(y=65, line_dash="dash", line_color="green")
        fig.add_vline(x=merged['median_price_sqft'].median(), line_dash="dash", line_color="blue")
        
        st.plotly_chart(fig)
        
        st.subheader("Top Opportunities")
        opp = merged[(merged['composite_score'] > 65) & (merged['median_price_sqft'] < merged['median_price_sqft'].median())]
        st.dataframe(opp[['ward_name', 'composite_score', 'median_price_sqft']].sort_values('composite_score', ascending=False))
    else:
        st.warning("Property price data not available.")

# ---- 7. City Overview ----
elif page == "7. City Overview":
    st.title(f"{city} Liveability Overview")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Average City Score", f"{df_scores_latest['composite_score'].mean():.1f}")
    
    top_ward = df_scores_latest.loc[df_scores_latest['composite_score'].idxmax()]
    bottom_ward = df_scores_latest.loc[df_scores_latest['composite_score'].idxmin()]
    
    col2.metric("Top Ward", top_ward['ward_name'], f"{top_ward['composite_score']:.1f}")
    col3.metric("Bottom Ward", bottom_ward['ward_name'], f"{bottom_ward['composite_score']:.1f}")
    
    fig = px.histogram(df_scores_latest, x="composite_score", nbins=20, title="Score Distribution")
    st.plotly_chart(fig)

# ---- 8. Ward Typology ----
elif page == "8. Ward Typology":
    st.title("Ward Clusters & Typology")
    
    if not df_clusters.empty:
        latest_clusters = df_clusters[df_clusters['year'] == df_clusters['year'].max()]
        
        fig = px.pie(latest_clusters, names='cluster_label', title="Cluster Distribution")
        st.plotly_chart(fig)
        
        # Show UMAP if exists
        umap_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml", "outputs", "umap_projection.csv")
        if os.path.exists(umap_path):
            umap_df = pd.read_csv(umap_path)
            fig_umap = px.scatter(umap_df, x="umap_x", y="umap_y", color="cluster_label", hover_name="ward_name", title="UMAP Projection of Ward Clusters")
            st.plotly_chart(fig_umap)
    else:
        st.warning("Cluster data not available.")

# ---- 9. Data Explorer ----
elif page == "9. Data Explorer":
    st.title("Data Explorer")
    
    score_min = st.slider("Minimum Composite Score", 0, 100, 0)
    filtered = df_scores_latest[df_scores_latest['composite_score'] >= score_min]
    
    st.dataframe(filtered[['ward_name', 'composite_score', 'safety_score', 'aqi_score']])
    
    csv = filtered.to_csv(index=False).encode('utf-8')
    st.download_button("Download CSV", csv, "explorer_export.csv", "text/csv")

# ---- 10. Tech Showcase ----
elif page == "10. Tech Showcase":
    st.title("Architecture & Tech Stack")
    
    st.markdown("""
    ### Data Pipeline
    * **Airflow** orchestration (7 DAGs)
    * **PostgreSQL + PostGIS** database
    * **dbt** for transformations
    
    ### Machine Learning
    * **Feature Engineering**: Pandas, Scikit-learn (KNN Imputer)
    * **Clustering**: KMeans + UMAP for Ward Typology
    * **Prediction**: XGBoost binary classifier (Decline Predictor)
    * **Explainability**: SHAP (TreeExplainer)
    
    ### Frontend
    * **Streamlit** + **Plotly** + **Folium**
    * **FastAPI** backend
    """)
