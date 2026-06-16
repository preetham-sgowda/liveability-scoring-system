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

# ── Elegant Light Theme CSS ──────────────────────────────────────────────
st.markdown("""
<style>
/* ── Google Fonts ─────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Root variables ───────────────────────────────────── */
:root {
    --primary: #6C63FF;
    --primary-light: #A5A0FF;
    --primary-soft: #EDE9FF;
    --accent-rose: #F87171;
    --accent-emerald: #34D399;
    --accent-amber: #FBBF24;
    --accent-sky: #38BDF8;
    --bg-main: #FAFBFE;
    --bg-card: #FFFFFF;
    --bg-sidebar: linear-gradient(180deg, #F5F3FF 0%, #EDE9FE 50%, #E0E7FF 100%);
    --text-primary: #2D3748;
    --text-secondary: #718096;
    --text-muted: #A0AEC0;
    --border-soft: #E8ECF4;
    --shadow-sm: 0 1px 3px rgba(108, 99, 255, 0.06);
    --shadow-md: 0 4px 12px rgba(108, 99, 255, 0.08);
    --shadow-lg: 0 8px 30px rgba(108, 99, 255, 0.10);
    --radius-sm: 8px;
    --radius-md: 12px;
    --radius-lg: 16px;
}

/* ── Global typography ────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
    color: var(--text-primary);
}

/* ── Main container ───────────────────────────────────── */
.stApp {
    background: var(--bg-main) !important;
}

/* ── Sidebar ──────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: var(--bg-sidebar) !important;
    border-right: 1px solid var(--border-soft) !important;
}

section[data-testid="stSidebar"] .stRadio > label {
    font-weight: 500;
    color: var(--text-primary) !important;
}

section[data-testid="stSidebar"] .stRadio > div > label {
    border-radius: var(--radius-sm) !important;
    padding: 6px 12px !important;
    margin-bottom: 2px !important;
    transition: all 0.2s ease !important;
}

section[data-testid="stSidebar"] .stRadio > div > label:hover {
    background: rgba(108, 99, 255, 0.08) !important;
}

section[data-testid="stSidebar"] .stRadio > div > label[data-selected="true"],
section[data-testid="stSidebar"] .stRadio > div > label[aria-checked="true"] {
    background: rgba(108, 99, 255, 0.12) !important;
    color: var(--primary) !important;
    font-weight: 600;
}

/* ── Headings ─────────────────────────────────────────── */
h1 {
    color: var(--text-primary) !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em !important;
    margin-bottom: 0.3em !important;
}

h2, h3 {
    color: var(--text-primary) !important;
    font-weight: 600 !important;
    letter-spacing: -0.01em !important;
}

/* ── Metric cards ─────────────────────────────────────── */
div[data-testid="stMetric"] {
    background: var(--bg-card) !important;
    border: 1px solid var(--border-soft) !important;
    border-radius: var(--radius-md) !important;
    padding: 20px 24px !important;
    box-shadow: var(--shadow-sm) !important;
    transition: all 0.3s ease !important;
}

div[data-testid="stMetric"]:hover {
    box-shadow: var(--shadow-md) !important;
    transform: translateY(-2px);
    border-color: var(--primary-light) !important;
}

div[data-testid="stMetric"] label {
    color: var(--text-secondary) !important;
    font-weight: 500 !important;
    font-size: 0.82rem !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: var(--primary) !important;
    font-weight: 700 !important;
    font-size: 1.8rem !important;
}

div[data-testid="stMetric"] [data-testid="stMetricDelta"] svg {
    display: inline;
}

/* ── Buttons ──────────────────────────────────────────── */
.stButton > button {
    background: linear-gradient(135deg, var(--primary) 0%, #8B83FF 100%) !important;
    color: white !important;
    border: none !important;
    border-radius: var(--radius-sm) !important;
    padding: 8px 24px !important;
    font-weight: 600 !important;
    font-size: 0.88rem !important;
    letter-spacing: 0.01em;
    box-shadow: 0 2px 8px rgba(108, 99, 255, 0.25) !important;
    transition: all 0.25s ease !important;
}

.stButton > button:hover {
    box-shadow: 0 4px 16px rgba(108, 99, 255, 0.35) !important;
    transform: translateY(-1px);
}

.stButton > button:active {
    transform: translateY(0);
}

/* ── Download button ──────────────────────────────────── */
.stDownloadButton > button {
    background: var(--bg-card) !important;
    color: var(--primary) !important;
    border: 1.5px solid var(--primary) !important;
    border-radius: var(--radius-sm) !important;
    font-weight: 600 !important;
    transition: all 0.25s ease !important;
}

.stDownloadButton > button:hover {
    background: var(--primary-soft) !important;
}

/* ── Selectbox & inputs ───────────────────────────────── */
.stSelectbox > div > div,
.stMultiSelect > div > div,
.stTextInput > div > div > input {
    border-radius: var(--radius-sm) !important;
    border-color: var(--border-soft) !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
}

.stSelectbox > div > div:focus-within,
.stMultiSelect > div > div:focus-within,
.stTextInput > div > div > input:focus {
    border-color: var(--primary) !important;
    box-shadow: 0 0 0 3px rgba(108, 99, 255, 0.12) !important;
}

/* ── Slider ───────────────────────────────────────────── */
.stSlider > div > div > div > div {
    background: var(--primary) !important;
}

/* ── Expanders ────────────────────────────────────────── */
.streamlit-expanderHeader {
    background: var(--bg-card) !important;
    border: 1px solid var(--border-soft) !important;
    border-radius: var(--radius-sm) !important;
    font-weight: 500 !important;
    transition: all 0.2s ease !important;
}

.streamlit-expanderHeader:hover {
    border-color: var(--primary-light) !important;
    background: #F8F7FF !important;
}

details {
    border: 1px solid var(--border-soft) !important;
    border-radius: var(--radius-md) !important;
    overflow: hidden;
}

/* ── Dataframes ───────────────────────────────────────── */
.stDataFrame {
    border: 1px solid var(--border-soft) !important;
    border-radius: var(--radius-md) !important;
    overflow: hidden !important;
    box-shadow: var(--shadow-sm) !important;
}

/* ── Progress bar ─────────────────────────────────────── */
.stProgress > div > div > div {
    background: linear-gradient(90deg, var(--primary) 0%, var(--primary-light) 100%) !important;
    border-radius: 10px !important;
}

/* ── Alerts & info boxes ──────────────────────────────── */
.stAlert {
    border-radius: var(--radius-md) !important;
    border: none !important;
}

div[data-testid="stNotification"] {
    border-radius: var(--radius-md) !important;
}

/* ── Plotly chart containers ──────────────────────────── */
.stPlotlyChart {
    background: var(--bg-card) !important;
    border: 1px solid var(--border-soft) !important;
    border-radius: var(--radius-md) !important;
    padding: 8px !important;
    box-shadow: var(--shadow-sm) !important;
}

/* ── Folium map container ─────────────────────────────── */
iframe {
    border-radius: var(--radius-md) !important;
    border: 1px solid var(--border-soft) !important;
    box-shadow: var(--shadow-sm) !important;
}

/* ── Markdown ─────────────────────────────────────────── */
.stMarkdown ul {
    color: var(--text-secondary);
}

.stMarkdown strong {
    color: var(--text-primary);
}

/* ── Column gaps ──────────────────────────────────────── */
div[data-testid="column"] {
    padding: 0 8px;
}

/* ── Scrollbar ────────────────────────────────────────── */
::-webkit-scrollbar {
    width: 6px;
    height: 6px;
}

::-webkit-scrollbar-track {
    background: transparent;
}

::-webkit-scrollbar-thumb {
    background: var(--text-muted);
    border-radius: 10px;
}

::-webkit-scrollbar-thumb:hover {
    background: var(--text-secondary);
}

/* ── Subtle fade-in animation ─────────────────────────── */
@keyframes fadeInUp {
    from { opacity: 0; transform: translateY(12px); }
    to { opacity: 1; transform: translateY(0); }
}

.main .block-container {
    animation: fadeInUp 0.4s ease-out;
    max-width: 1200px;
}
</style>
""", unsafe_allow_html=True)

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Paths for CSV fallback
PROCESSED_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "processed")
GEOJSON_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "wards", "bengaluru_wards.geojson")

# Helper to load data
@st.cache_data
def load_data(table_name):
    """Load data from database or fallback CSV."""
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

# ── Plotly Elegant Light Template ─────────────────────────────────────────
THEME_COLORS = ['#6C63FF', '#38BDF8', '#34D399', '#FBBF24', '#F87171', '#A78BFA', '#FB923C', '#2DD4BF']

LIGHT_TEMPLATE = go.layout.Template(
    layout=go.Layout(
        font=dict(family='Inter, sans-serif', color='#2D3748'),
        paper_bgcolor='#FFFFFF',
        plot_bgcolor='#FAFBFE',
        title=dict(font=dict(size=18, color='#2D3748', family='Inter, sans-serif'), x=0.02),
        colorway=THEME_COLORS,
        xaxis=dict(gridcolor='#EDF2F7', linecolor='#E2E8F0', zerolinecolor='#E2E8F0'),
        yaxis=dict(gridcolor='#EDF2F7', linecolor='#E2E8F0', zerolinecolor='#E2E8F0'),
        legend=dict(bgcolor='rgba(255,255,255,0)', font=dict(size=12)),
        margin=dict(l=40, r=20, t=50, b=40),
        hoverlabel=dict(bgcolor='#FFFFFF', font_size=13, font_family='Inter, sans-serif', bordercolor='#E8ECF4'),
    )
)

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
    "10. Tech Showcase",
    "11. Ward Directory"
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
            with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
                geojson_data = json.load(f)
                
            m = folium.Map(location=[12.9716, 77.5946], zoom_start=11, tiles="CartoDB positron")
            
            # Map score to color
            def get_color(score):
                if score >= 70: return "#2ecc71" # Green
                elif score >= 40: return "#f1c40f" # Yellow
                else: return "#e74c3c" # Red
                
            folium.Choropleth(
                geo_data=geojson_data,
                name="Liveability Score",
                data=df_scores_latest,
                columns=["ward_name", "composite_score"],
                key_on="feature.properties.name_en",
                fill_color="PuBuGn",
                fill_opacity=0.65,
                line_opacity=0.15,
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
        fig.add_trace(go.Scatterpolar(
            r=values, theta=categories, fill='toself', name=selected_ward,
            fillcolor='rgba(108, 99, 255, 0.15)', line=dict(color='#6C63FF', width=2.5),
            marker=dict(color='#6C63FF', size=6)
        ))
        fig.update_layout(
            template=LIGHT_TEMPLATE,
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 100], gridcolor='#EDF2F7', linecolor='#E2E8F0'),
                angularaxis=dict(gridcolor='#EDF2F7', linecolor='#E2E8F0'),
                bgcolor='#FAFBFE'
            ),
            showlegend=False
        )
        st.plotly_chart(fig)
        
        st.subheader("Historical Trend")
        fig_trend = px.line(ward_data, x="year", y="composite_score", markers=True, title="Composite Score Over Time",
                           color_discrete_sequence=['#6C63FF'])
        fig_trend.update_layout(template=LIGHT_TEMPLATE)
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
    fig.add_trace(go.Bar(x=comp_df['Dimension'], y=comp_df[ward_a], name=ward_a,
                        marker_color='#6C63FF', marker_line_width=0, opacity=0.85))
    fig.add_trace(go.Bar(x=comp_df['Dimension'], y=comp_df[ward_b], name=ward_b,
                        marker_color='#38BDF8', marker_line_width=0, opacity=0.85))
    fig.update_layout(template=LIGHT_TEMPLATE, barmode='group', title="Dimension Comparison")
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
        fig = px.line(trend_data, x="year", y="composite_score", color="ward_name", title="Composite Score Trends",
                     color_discrete_sequence=THEME_COLORS, markers=True)
        fig.update_layout(template=LIGHT_TEMPLATE)
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
                        labels={"median_price_sqft": "Price per Sqft", "composite_score": "Liveability Score"},
                        color_discrete_sequence=['#6C63FF'])
        fig.update_traces(marker=dict(size=10, line=dict(width=1, color='#FFFFFF'), opacity=0.8))
        fig.update_layout(template=LIGHT_TEMPLATE)
        
        # Quadrant lines with soft theme colors
        fig.add_hline(y=65, line_dash="dash", line_color='#34D399', line_width=1.5)
        fig.add_vline(x=merged['median_price_sqft'].median(), line_dash="dash", line_color='#A78BFA', line_width=1.5)
        
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
    
    fig = px.histogram(df_scores_latest, x="composite_score", nbins=20, title="Score Distribution",
                       color_discrete_sequence=['#6C63FF'], opacity=0.8)
    fig.update_layout(template=LIGHT_TEMPLATE)
    st.plotly_chart(fig)

# ---- 8. Ward Typology ----
elif page == "8. Ward Typology":
    st.title("Ward Clusters & Typology")
    
    if not df_clusters.empty:
        latest_clusters = df_clusters[df_clusters['year'] == df_clusters['year'].max()]
        
        fig = px.pie(latest_clusters, names='cluster_label', title="Cluster Distribution",
                    color_discrete_sequence=THEME_COLORS, hole=0.4)
        fig.update_traces(textinfo='percent+label', textfont_size=12, marker=dict(line=dict(color='#FFFFFF', width=2)))
        fig.update_layout(template=LIGHT_TEMPLATE)
        st.plotly_chart(fig)
        
        # Show UMAP if exists
        umap_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml", "outputs", "umap_projection.csv")
        if os.path.exists(umap_path):
            umap_df = pd.read_csv(umap_path)
            fig_umap = px.scatter(umap_df, x="umap_x", y="umap_y", color="cluster_label", hover_name="ward_name",
                                  title="UMAP Projection of Ward Clusters", color_discrete_sequence=THEME_COLORS)
            fig_umap.update_traces(marker=dict(size=8, line=dict(width=1, color='#FFFFFF'), opacity=0.85))
            fig_umap.update_layout(template=LIGHT_TEMPLATE)
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

# ---- 11. Ward Directory ----
elif page == "11. Ward Directory":
    st.title("Ward Directory")
    st.markdown("Easily look up the ward name corresponding to each ward ID.")
    
    # Get unique ward mapping
    ward_mapping = df_scores[['ward_id', 'ward_name']].drop_duplicates().sort_values('ward_id').reset_index(drop=True)
    
    # Add a search bar to easily filter the dataframe
    search_query = st.text_input("Search by Ward ID or Name", "")
    if search_query:
        ward_mapping = ward_mapping[
            ward_mapping['ward_name'].str.contains(search_query, case=False, na=False) |
            ward_mapping['ward_id'].astype(str).str.contains(search_query, case=False, na=False)
        ]
        
    st.dataframe(ward_mapping, use_container_width=True, hide_index=True)
