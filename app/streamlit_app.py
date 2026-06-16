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

# ── Vibrant Elegant Theme CSS ────────────────────────────────────────────
st.markdown("""
<style>
/* ── Google Fonts ─────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');

/* ── Root variables ───────────────────────────────────── */
:root {
    --primary: #6366F1;
    --primary-hover: #4F46E5;
    --primary-light: #A5B4FC;
    --primary-glow: rgba(99, 102, 241, 0.4);
    --accent-violet: #8B5CF6;
    --accent-rose: #F43F5E;
    --accent-emerald: #10B981;
    --accent-amber: #F59E0B;
    --accent-sky: #0EA5E9;
    --accent-teal: #14B8A6;
    --accent-pink: #EC4899;
    --accent-orange: #F97316;
    --bg-main: #F8FAFF;
    --bg-card: rgba(255, 255, 255, 0.85);
    --bg-card-solid: #FFFFFF;
    --text-primary: #1E293B;
    --text-secondary: #64748B;
    --text-muted: #94A3B8;
    --border-soft: rgba(148, 163, 184, 0.2);
    --border-glow: rgba(99, 102, 241, 0.3);
    --shadow-sm: 0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06);
    --shadow-md: 0 4px 6px -1px rgba(0,0,0,0.07), 0 2px 4px -1px rgba(0,0,0,0.04);
    --shadow-lg: 0 10px 25px -3px rgba(0,0,0,0.08), 0 4px 10px -2px rgba(0,0,0,0.04);
    --shadow-glow: 0 0 20px rgba(99, 102, 241, 0.15);
    --radius-sm: 10px;
    --radius-md: 14px;
    --radius-lg: 20px;
    --radius-xl: 24px;
}

/* ── Global typography ────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
    color: var(--text-primary);
}

/* ── Main container with subtle animated gradient bg ──── */
.stApp {
    background: linear-gradient(135deg, #F8FAFF 0%, #EEF2FF 25%, #FDF2F8 50%, #ECFDF5 75%, #F8FAFF 100%) !important;
    background-size: 400% 400% !important;
    animation: gradientShift 20s ease infinite !important;
}

@keyframes gradientShift {
    0%   { background-position: 0% 50%; }
    25%  { background-position: 50% 0%; }
    50%  { background-position: 100% 50%; }
    75%  { background-position: 50% 100%; }
    100% { background-position: 0% 50%; }
}

/* ── Sidebar ──────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #312E81 0%, #4338CA 40%, #6366F1 100%) !important;
    border-right: none !important;
    box-shadow: 4px 0 24px rgba(99, 102, 241, 0.15) !important;
}

section[data-testid="stSidebar"]::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; bottom: 0;
    background: radial-gradient(circle at 20% 80%, rgba(139, 92, 246, 0.3) 0%, transparent 50%),
                radial-gradient(circle at 80% 20%, rgba(14, 165, 233, 0.2) 0%, transparent 50%);
    pointer-events: none;
}

section[data-testid="stSidebar"] * {
    color: rgba(255, 255, 255, 0.92) !important;
}

section[data-testid="stSidebar"] .stRadio > label {
    font-weight: 600 !important;
    font-size: 0.75rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    color: rgba(255, 255, 255, 0.55) !important;
}

section[data-testid="stSidebar"] .stRadio > div > label {
    border-radius: var(--radius-sm) !important;
    padding: 8px 14px !important;
    margin-bottom: 3px !important;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    border: 1px solid transparent !important;
    font-size: 0.9rem !important;
}

section[data-testid="stSidebar"] .stRadio > div > label:hover {
    background: rgba(255, 255, 255, 0.12) !important;
    border-color: rgba(255, 255, 255, 0.15) !important;
    transform: translateX(4px);
}

section[data-testid="stSidebar"] .stRadio > div > label[data-selected="true"],
section[data-testid="stSidebar"] .stRadio > div > label[aria-checked="true"] {
    background: rgba(255, 255, 255, 0.18) !important;
    border-color: rgba(255, 255, 255, 0.3) !important;
    color: #FFFFFF !important;
    font-weight: 700 !important;
    box-shadow: 0 2px 10px rgba(0,0,0,0.15) !important;
}

section[data-testid="stSidebar"] .stSelectbox > div > div {
    background: rgba(255, 255, 255, 0.1) !important;
    border-color: rgba(255, 255, 255, 0.2) !important;
    border-radius: var(--radius-sm) !important;
}

section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    color: #FFFFFF !important;
}

/* ── Headings with gradient text ──────────────────────── */
h1 {
    background: linear-gradient(135deg, #312E81 0%, #6366F1 40%, #8B5CF6 70%, #EC4899 100%) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
    font-weight: 800 !important;
    letter-spacing: -0.03em !important;
    margin-bottom: 0.2em !important;
    font-size: 2.1rem !important;
}

h2 {
    color: var(--text-primary) !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em !important;
    position: relative;
    display: inline-block;
}

h3 {
    color: var(--text-primary) !important;
    font-weight: 600 !important;
    letter-spacing: -0.01em !important;
}

/* ── Metric cards — glassmorphism + colored accents ───── */
div[data-testid="stMetric"] {
    background: var(--bg-card) !important;
    backdrop-filter: blur(12px) !important;
    -webkit-backdrop-filter: blur(12px) !important;
    border: 1px solid var(--border-soft) !important;
    border-radius: var(--radius-lg) !important;
    padding: 22px 26px !important;
    box-shadow: var(--shadow-sm) !important;
    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1) !important;
    position: relative !important;
    overflow: hidden !important;
}

div[data-testid="stMetric"]::before {
    content: '';
    position: absolute;
    top: 0; left: 0;
    width: 4px; height: 100%;
    background: linear-gradient(180deg, var(--primary) 0%, var(--accent-violet) 100%);
    border-radius: 4px 0 0 4px;
}

div[data-testid="stMetric"]::after {
    content: '';
    position: absolute;
    top: -50%; right: -50%;
    width: 100px; height: 100px;
    background: radial-gradient(circle, rgba(99, 102, 241, 0.06) 0%, transparent 70%);
    border-radius: 50%;
    transition: all 0.4s ease;
}

div[data-testid="stMetric"]:hover {
    box-shadow: var(--shadow-lg), var(--shadow-glow) !important;
    transform: translateY(-4px) !important;
    border-color: var(--border-glow) !important;
}

div[data-testid="stMetric"]:hover::after {
    transform: scale(2);
    opacity: 0.8;
}

div[data-testid="stMetric"] label {
    color: var(--text-secondary) !important;
    font-weight: 600 !important;
    font-size: 0.78rem !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}

div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    background: linear-gradient(135deg, var(--primary) 0%, var(--accent-violet) 100%) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
    font-weight: 800 !important;
    font-size: 2rem !important;
}

div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
    font-weight: 600 !important;
}

/* ── Color-code metric cards per column ───────────────── */
div[data-testid="column"]:nth-child(1) div[data-testid="stMetric"]::before {
    background: linear-gradient(180deg, #6366F1, #8B5CF6) !important;
}
div[data-testid="column"]:nth-child(2) div[data-testid="stMetric"]::before {
    background: linear-gradient(180deg, #10B981, #14B8A6) !important;
}
div[data-testid="column"]:nth-child(3) div[data-testid="stMetric"]::before {
    background: linear-gradient(180deg, #F59E0B, #F97316) !important;
}
div[data-testid="column"]:nth-child(2) div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    background: linear-gradient(135deg, #10B981, #14B8A6) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
}
div[data-testid="column"]:nth-child(3) div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    background: linear-gradient(135deg, #F59E0B, #F97316) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
}

/* ── Buttons — vivid gradient with glow ───────────────── */
.stButton > button {
    background: linear-gradient(135deg, #6366F1 0%, #8B5CF6 50%, #A855F7 100%) !important;
    color: white !important;
    border: none !important;
    border-radius: 50px !important;
    padding: 10px 28px !important;
    font-weight: 700 !important;
    font-size: 0.88rem !important;
    letter-spacing: 0.02em;
    box-shadow: 0 4px 15px rgba(99, 102, 241, 0.35) !important;
    transition: all 0.35s cubic-bezier(0.4, 0, 0.2, 1) !important;
    position: relative;
    overflow: hidden;
}

.stButton > button::before {
    content: '';
    position: absolute;
    top: 0; left: -100%;
    width: 100%; height: 100%;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
    transition: left 0.5s ease;
}

.stButton > button:hover {
    box-shadow: 0 6px 25px rgba(99, 102, 241, 0.5) !important;
    transform: translateY(-2px) scale(1.02) !important;
}

.stButton > button:hover::before {
    left: 100%;
}

.stButton > button:active {
    transform: translateY(0) scale(0.98) !important;
}

/* ── Download button ──────────────────────────────────── */
.stDownloadButton > button {
    background: transparent !important;
    color: var(--primary) !important;
    border: 2px solid var(--primary) !important;
    border-radius: 50px !important;
    font-weight: 700 !important;
    transition: all 0.3s ease !important;
}

.stDownloadButton > button:hover {
    background: var(--primary) !important;
    color: white !important;
    box-shadow: 0 4px 15px rgba(99, 102, 241, 0.3) !important;
}

/* ── Selectbox & inputs ───────────────────────────────── */
.stSelectbox > div > div,
.stMultiSelect > div > div,
.stTextInput > div > div > input {
    border-radius: var(--radius-sm) !important;
    border: 1.5px solid var(--border-soft) !important;
    background: var(--bg-card) !important;
    backdrop-filter: blur(8px) !important;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
}

.stSelectbox > div > div:focus-within,
.stMultiSelect > div > div:focus-within,
.stTextInput > div > div > input:focus {
    border-color: var(--primary) !important;
    box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.15), var(--shadow-md) !important;
}

/* ── Slider — gradient track ──────────────────────────── */
.stSlider > div > div > div > div {
    background: linear-gradient(90deg, var(--primary) 0%, var(--accent-violet) 100%) !important;
}

.stSlider [data-testid="stThumbValue"] {
    color: var(--primary) !important;
    font-weight: 700 !important;
}

/* ── Expanders — elegant card style ───────────────────── */
.streamlit-expanderHeader {
    background: var(--bg-card) !important;
    backdrop-filter: blur(8px) !important;
    border: 1.5px solid var(--border-soft) !important;
    border-radius: var(--radius-md) !important;
    font-weight: 600 !important;
    transition: all 0.3s ease !important;
    padding: 10px 16px !important;
}

.streamlit-expanderHeader:hover {
    border-color: var(--primary-light) !important;
    background: rgba(99, 102, 241, 0.04) !important;
    box-shadow: var(--shadow-md) !important;
}

details {
    border: 1.5px solid var(--border-soft) !important;
    border-radius: var(--radius-md) !important;
    overflow: hidden;
    background: var(--bg-card) !important;
    backdrop-filter: blur(8px) !important;
    margin-bottom: 8px !important;
    transition: all 0.3s ease !important;
}

details:hover {
    border-color: var(--border-glow) !important;
    box-shadow: var(--shadow-md), 0 0 15px rgba(99, 102, 241, 0.08) !important;
}

details[open] {
    border-color: var(--primary-light) !important;
    box-shadow: var(--shadow-lg) !important;
}

/* ── Dataframes ───────────────────────────────────────── */
.stDataFrame {
    border: 1.5px solid var(--border-soft) !important;
    border-radius: var(--radius-md) !important;
    overflow: hidden !important;
    box-shadow: var(--shadow-md) !important;
}

/* ── Progress bar — animated gradient ─────────────────── */
.stProgress > div > div > div {
    background: linear-gradient(90deg, #F43F5E 0%, #F59E0B 40%, #10B981 100%) !important;
    border-radius: 10px !important;
    box-shadow: 0 2px 8px rgba(244, 63, 94, 0.3) !important;
    animation: progressPulse 2s ease-in-out infinite !important;
}

@keyframes progressPulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.85; }
}

/* ── Alerts & info boxes ──────────────────────────────── */
.stAlert {
    border-radius: var(--radius-md) !important;
    border-left: 4px solid !important;
    backdrop-filter: blur(8px) !important;
}

div[data-testid="stNotification"] {
    border-radius: var(--radius-md) !important;
}

/* ── Plotly chart containers — glassmorphism ──────────── */
.stPlotlyChart {
    background: var(--bg-card) !important;
    backdrop-filter: blur(12px) !important;
    -webkit-backdrop-filter: blur(12px) !important;
    border: 1.5px solid var(--border-soft) !important;
    border-radius: var(--radius-lg) !important;
    padding: 12px !important;
    box-shadow: var(--shadow-md) !important;
    transition: all 0.4s ease !important;
}

.stPlotlyChart:hover {
    box-shadow: var(--shadow-lg), 0 0 20px rgba(99, 102, 241, 0.08) !important;
    border-color: var(--border-glow) !important;
}

/* ── Folium map container ─────────────────────────────── */
iframe {
    border-radius: var(--radius-lg) !important;
    border: 1.5px solid var(--border-soft) !important;
    box-shadow: var(--shadow-lg) !important;
}

/* ── Markdown ─────────────────────────────────────────── */
.stMarkdown ul {
    color: var(--text-secondary);
}

.stMarkdown strong {
    color: var(--text-primary);
    font-weight: 700;
}

/* ── Column gaps ──────────────────────────────────────── */
div[data-testid="column"] {
    padding: 0 10px;
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
    background: linear-gradient(180deg, var(--primary-light), var(--accent-violet));
    border-radius: 10px;
}

::-webkit-scrollbar-thumb:hover {
    background: var(--primary);
}

/* ── Animations ───────────────────────────────────────── */
@keyframes fadeInUp {
    from { opacity: 0; transform: translateY(20px); }
    to { opacity: 1; transform: translateY(0); }
}

@keyframes slideInLeft {
    from { opacity: 0; transform: translateX(-20px); }
    to { opacity: 1; transform: translateX(0); }
}

.main .block-container {
    animation: fadeInUp 0.5s cubic-bezier(0.4, 0, 0.2, 1);
    max-width: 1200px;
}

/* ── Stagger animation for columns ────────────────────── */
div[data-testid="column"]:nth-child(1) { animation: fadeInUp 0.4s ease 0s both; }
div[data-testid="column"]:nth-child(2) { animation: fadeInUp 0.4s ease 0.1s both; }
div[data-testid="column"]:nth-child(3) { animation: fadeInUp 0.4s ease 0.2s both; }
div[data-testid="column"]:nth-child(4) { animation: fadeInUp 0.4s ease 0.3s both; }

/* ── Separator / divider styling ──────────────────────── */
hr {
    border: none !important;
    height: 2px !important;
    background: linear-gradient(90deg, transparent, var(--primary-light), var(--accent-violet), transparent) !important;
    margin: 1.5rem 0 !important;
}

/* ── Tab styling ──────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px !important;
    background: transparent !important;
}

.stTabs [data-baseweb="tab"] {
    border-radius: var(--radius-sm) !important;
    font-weight: 600 !important;
    transition: all 0.3s ease !important;
}

.stTabs [aria-selected="true"] {
    background: var(--primary) !important;
    color: white !important;
    border-radius: var(--radius-sm) !important;
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

# ── Plotly Vibrant Light Template ─────────────────────────────────────────
THEME_COLORS = ['#6366F1', '#0EA5E9', '#10B981', '#F59E0B', '#F43F5E', '#8B5CF6', '#F97316', '#14B8A6', '#EC4899', '#06B6D4']

LIGHT_TEMPLATE = go.layout.Template(
    layout=go.Layout(
        font=dict(family='Plus Jakarta Sans, sans-serif', color='#1E293B', size=13),
        paper_bgcolor='rgba(255,255,255,0)',
        plot_bgcolor='rgba(248,250,255,0.6)',
        title=dict(font=dict(size=20, color='#1E293B', family='Plus Jakarta Sans, sans-serif'), x=0.02, y=0.95),
        colorway=THEME_COLORS,
        xaxis=dict(gridcolor='rgba(148,163,184,0.15)', linecolor='rgba(148,163,184,0.2)', zerolinecolor='rgba(148,163,184,0.2)',
                   gridwidth=1, title_font=dict(size=13, color='#64748B')),
        yaxis=dict(gridcolor='rgba(148,163,184,0.15)', linecolor='rgba(148,163,184,0.2)', zerolinecolor='rgba(148,163,184,0.2)',
                   gridwidth=1, title_font=dict(size=13, color='#64748B')),
        legend=dict(bgcolor='rgba(255,255,255,0)', font=dict(size=12, color='#64748B'),
                    bordercolor='rgba(0,0,0,0)', orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        margin=dict(l=40, r=20, t=60, b=40),
        hoverlabel=dict(bgcolor='#FFFFFF', font_size=13, font_family='Plus Jakarta Sans, sans-serif',
                       bordercolor='rgba(99,102,241,0.3)'),
    )
)

# ── Helper: styled page subtitle ─────────────────────────────────────────
def page_subtitle(text):
    st.markdown(f'<p style="color:#64748B; font-size:1.05rem; margin-top:-8px; margin-bottom:20px; font-weight:400;">{text}</p>', unsafe_allow_html=True)

def gradient_divider():
    st.markdown('<hr style="border:none; height:2px; background:linear-gradient(90deg, transparent, #A5B4FC, #8B5CF6, #EC4899, transparent); margin:1.5rem 0;">', unsafe_allow_html=True)

# Sidebar navigation
st.sidebar.markdown('<h2 style="font-size:1.4rem; font-weight:800; color:white !important; margin-bottom:2px;">🏙️ Liveability</h2>', unsafe_allow_html=True)
st.sidebar.markdown('<p style="font-size:0.82rem; color:rgba(255,255,255,0.5) !important; margin-top:0; margin-bottom:16px; letter-spacing:0.1em; text-transform:uppercase;">Scoring System</p>', unsafe_allow_html=True)
city = st.sidebar.selectbox("Select City", ["Bengaluru", "Mumbai", "Delhi"], index=0)
st.sidebar.markdown('---')

pages = [
    "🗺️  Landing Map",
    "🔍  Ward Details",
    "⚖️  Compare Wards",
    "🚨  Declining Alerts",
    "📈  Trends Dashboard",
    "💎  Opportunity Wards",
    "🏛️  City Overview",
    "🧬  Ward Typology",
    "📊  Data Explorer",
    "⚙️  Tech Showcase",
    "📋  Ward Directory"
]

page = st.sidebar.radio("Navigation", pages)

if df_scores.empty or df_features.empty:
    st.error("Data could not be loaded. Please ensure data pipelines have been run.")
    st.stop()

# Filter for latest year for static pages
latest_year = df_scores['year'].max()
df_scores_latest = df_scores[df_scores['year'] == latest_year]

# ---- 1. Landing Map ----
if page == "🗺️  Landing Map":
    st.title(f"Liveability Map — {city}")
    page_subtitle(f"Interactive choropleth of ward-level liveability scores for {latest_year}")
    
    # Quick stats row
    avg_score = df_scores_latest['composite_score'].mean()
    top_w = df_scores_latest.loc[df_scores_latest['composite_score'].idxmax()]
    num_wards = len(df_scores_latest)
    qs1, qs2, qs3 = st.columns(3)
    qs1.metric("📊 City Average", f"{avg_score:.1f}")
    qs2.metric("🏆 Top Ward", top_w['ward_name'])
    qs3.metric("🗺️ Total Wards", f"{num_wards}")
    
    gradient_divider()
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.subheader("🔎 Search Ward")
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
elif page == "🔍  Ward Details":
    st.title("Ward Deep Dive")
    page_subtitle("Explore detailed liveability dimensions for any ward")
    
    ward_names = sorted(df_scores['ward_name'].unique())
    selected_ward = st.selectbox("Select Ward", ward_names)
    
    ward_data = df_scores[df_scores['ward_name'] == selected_ward].sort_values('year')
    if not ward_data.empty:
        latest_data = ward_data.iloc[-1]
        
        # Score overview — all dimension metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("🎯 Composite Score", f"{latest_data['composite_score']:.1f}/100", 
                   f"{latest_data['composite_score'] - ward_data.iloc[-2]['composite_score']:.1f}" if len(ward_data) > 1 else "0")
        col2.metric("🛡️ Safety", f"{latest_data['safety_score']:.1f}")
        col3.metric("🌬️ Air Quality", f"{latest_data['aqi_score']:.1f}")
        
        col4, col5, col6 = st.columns(3)
        col4.metric("🏗️ Civic Infra", f"{latest_data['civic_score']:.1f}")
        col5.metric("🚌 Transit", f"{latest_data['transit_score']:.1f}")
        col6.metric("🌳 Green Cover", f"{latest_data['green_score']:.1f}")
        
        gradient_divider()
        
        st.subheader("Dimension Radar")
        categories = ['Safety', 'AQI', 'Civic', 'Transit', 'Green', 'Affordability']
        values = [latest_data['safety_score'], latest_data['aqi_score'], latest_data['civic_score'], 
                  latest_data['transit_score'], latest_data['green_score'], latest_data['affordability_score']]
        
        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=values, theta=categories, fill='toself', name=selected_ward,
            fillcolor='rgba(99, 102, 241, 0.18)', line=dict(color='#6366F1', width=3),
            marker=dict(color='#6366F1', size=7)
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
                           color_discrete_sequence=['#6366F1'])
        fig_trend.update_traces(line=dict(width=3), marker=dict(size=9, line=dict(width=2, color='#FFFFFF')))
        fig_trend.update_layout(template=LIGHT_TEMPLATE)
        st.plotly_chart(fig_trend, use_container_width=True)

# ---- 3. Compare Wards ----
elif page == "⚖️  Compare Wards":
    st.title("Compare Wards")
    page_subtitle("Side-by-side comparison of liveability dimensions between two wards")
    
    ward_names = sorted(df_scores['ward_name'].unique())
    col1, col2 = st.columns(2)
    with col1: ward_a = st.selectbox("Ward A", ward_names, index=0)
    with col2: ward_b = st.selectbox("Ward B", ward_names, index=1 if len(ward_names)>1 else 0)
    
    # Score badges
    a_data = df_scores_latest[df_scores_latest['ward_name'] == ward_a].iloc[0]
    b_data = df_scores_latest[df_scores_latest['ward_name'] == ward_b].iloc[0]
    
    sc1, sc2 = st.columns(2)
    sc1.metric(f"🏅 {ward_a}", f"{a_data['composite_score']:.1f}/100")
    sc2.metric(f"🏅 {ward_b}", f"{b_data['composite_score']:.1f}/100")
    
    gradient_divider()
    
    dims = ['composite_score', 'safety_score', 'aqi_score', 'civic_score', 'transit_score', 'green_score', 'affordability_score']
    dim_labels = ['Composite', 'Safety', 'AQI', 'Civic', 'Transit', 'Green', 'Affordability']
    
    comp_df = pd.DataFrame({
        'Dimension': dim_labels,
        ward_a: [a_data[d] for d in dims],
        ward_b: [b_data[d] for d in dims]
    })
    
    # Plotly grouped bar
    fig = go.Figure()
    fig.add_trace(go.Bar(x=comp_df['Dimension'], y=comp_df[ward_a], name=ward_a,
                        marker_color='#6366F1', marker_line_width=0, opacity=0.9,
                        marker=dict(cornerradius=6)))
    fig.add_trace(go.Bar(x=comp_df['Dimension'], y=comp_df[ward_b], name=ward_b,
                        marker_color='#0EA5E9', marker_line_width=0, opacity=0.9,
                        marker=dict(cornerradius=6)))
    fig.update_layout(template=LIGHT_TEMPLATE, barmode='group', title="Dimension Comparison")
    st.plotly_chart(fig, use_container_width=True)

# ---- 4. Declining Ward Alerts ----
elif page == "🚨  Declining Alerts":
    st.title("Declining Ward Alerts")
    page_subtitle("ML-powered predictions of wards at risk of liveability decline")
    
    if not df_decline.empty and not df_shap.empty:
        latest_decline = df_decline[df_decline['year'] == df_decline['year'].max()]
        
        # Merge with names and SHAP
        alerts = pd.merge(latest_decline, df_scores_latest[['ward_id', 'ward_name']], on='ward_id')
        alerts = pd.merge(alerts, df_shap[['ward_id', 'driver_1_feature', 'driver_2_feature']], on='ward_id')
        
        alerts = alerts.sort_values('decline_probability', ascending=False)
        
        # Summary metrics
        high_risk = len(alerts[alerts['decline_probability'] >= 0.7])
        med_risk = len(alerts[(alerts['decline_probability'] >= 0.4) & (alerts['decline_probability'] < 0.7)])
        low_risk = len(alerts[alerts['decline_probability'] < 0.4])
        m1, m2, m3 = st.columns(3)
        m1.metric("🔴 High Risk", high_risk)
        m2.metric("🟡 Medium Risk", med_risk)
        m3.metric("🟢 Low Risk", low_risk)
        
        gradient_divider()
        
        st.subheader("Top At-Risk Wards")
        threshold = st.slider("Risk Threshold", 0.0, 1.0, 0.5)
        filtered = alerts[alerts['decline_probability'] >= threshold]
        
        for _, row in filtered.head(10).iterrows():
            risk_pct = row['decline_probability']
            risk_emoji = "🔴" if risk_pct >= 0.7 else ("🟡" if risk_pct >= 0.4 else "🟢")
            with st.expander(f"{risk_emoji} {row['ward_name']} — Risk: {risk_pct:.1%}"):
                st.progress(risk_pct)
                st.write(f"**Top Risk Drivers:** 1. `{row['driver_1_feature']}` | 2. `{row['driver_2_feature']}`")
    else:
        st.warning("Decline prediction data not available.")

# ---- 5. Trends Dashboard ----
elif page == "📈  Trends Dashboard":
    st.title("Temporal Trends")
    page_subtitle("Track how ward liveability scores evolve over time")
    
    ward_names = sorted(df_scores['ward_name'].unique())
    selected_wards = st.multiselect("Select Wards to Compare", ward_names, default=[ward_names[0]])
    
    trend_data = df_scores[df_scores['ward_name'].isin(selected_wards)]
    
    if not trend_data.empty:
        fig = px.line(trend_data, x="year", y="composite_score", color="ward_name", title="Composite Score Trends",
                     color_discrete_sequence=THEME_COLORS, markers=True)
        fig.update_traces(line=dict(width=3), marker=dict(size=8, line=dict(width=2, color='#FFFFFF')))
        fig.update_layout(template=LIGHT_TEMPLATE)
        st.plotly_chart(fig, use_container_width=True)

# ---- 6. Opportunity Wards ----
elif page == "💎  Opportunity Wards":
    st.title("Opportunity Wards")
    page_subtitle("Discover high-liveability wards with below-median property prices — hidden gems for investors and residents")
    
    if 'median_price_sqft' in df_features.columns:
        latest_feat = df_features[df_features['year'] == latest_year]
        merged = pd.merge(df_scores_latest, latest_feat[['ward_id', 'median_price_sqft']], on='ward_id')
        
        merged = merged[merged['median_price_sqft'] > 0]
        
        fig = px.scatter(merged, x="median_price_sqft", y="composite_score", hover_name="ward_name",
                        title="Liveability vs. Property Price",
                        labels={"median_price_sqft": "Price per Sqft ₹", "composite_score": "Liveability Score"},
                        color_discrete_sequence=['#6366F1'], size='composite_score', size_max=18)
        fig.update_traces(marker=dict(line=dict(width=1.5, color='#FFFFFF'), opacity=0.85))
        fig.update_layout(template=LIGHT_TEMPLATE)
        
        # Quadrant lines
        fig.add_hline(y=65, line_dash="dash", line_color='#10B981', line_width=1.5,
                     annotation_text="High Liveability", annotation_position="top left")
        fig.add_vline(x=merged['median_price_sqft'].median(), line_dash="dash", line_color='#8B5CF6', line_width=1.5,
                     annotation_text="Median Price", annotation_position="top right")
        
        st.plotly_chart(fig, use_container_width=True)
        
        gradient_divider()
        st.subheader("💎 Top Opportunities")
        opp = merged[(merged['composite_score'] > 65) & (merged['median_price_sqft'] < merged['median_price_sqft'].median())]
        st.dataframe(opp[['ward_name', 'composite_score', 'median_price_sqft']].sort_values('composite_score', ascending=False),
                    use_container_width=True, hide_index=True)
    else:
        st.warning("Property price data not available.")

# ---- 7. City Overview ----
elif page == "🏛️  City Overview":
    st.title(f"{city} Liveability Overview")
    page_subtitle(f"High-level snapshot of liveability across all wards in {city}")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("📊 Average Score", f"{df_scores_latest['composite_score'].mean():.1f}")
    
    top_ward = df_scores_latest.loc[df_scores_latest['composite_score'].idxmax()]
    bottom_ward = df_scores_latest.loc[df_scores_latest['composite_score'].idxmin()]
    
    col2.metric("🏆 Best Ward", top_ward['ward_name'], f"{top_ward['composite_score']:.1f}")
    col3.metric("⚠️ Needs Attention", bottom_ward['ward_name'], f"{bottom_ward['composite_score']:.1f}")
    
    gradient_divider()
    
    fig = px.histogram(df_scores_latest, x="composite_score", nbins=20, title="Score Distribution Across All Wards",
                       color_discrete_sequence=['#6366F1'], opacity=0.85)
    fig.update_traces(marker=dict(line=dict(width=1, color='#FFFFFF'), cornerradius=4))
    fig.update_layout(template=LIGHT_TEMPLATE)
    st.plotly_chart(fig, use_container_width=True)

# ---- 8. Ward Typology ----
elif page == "🧬  Ward Typology":
    st.title("Ward Clusters & Typology")
    page_subtitle("ML-discovered ward archetypes using KMeans clustering and UMAP dimensionality reduction")
    
    if not df_clusters.empty:
        latest_clusters = df_clusters[df_clusters['year'] == df_clusters['year'].max()]
        
        c1, c2 = st.columns([1, 1])
        with c1:
            fig = px.pie(latest_clusters, names='cluster_label', title="Cluster Distribution",
                        color_discrete_sequence=THEME_COLORS, hole=0.45)
            fig.update_traces(textinfo='percent+label', textfont_size=13,
                            marker=dict(line=dict(color='#FFFFFF', width=2.5)),
                            pull=[0.03]*len(latest_clusters['cluster_label'].unique()))
            fig.update_layout(template=LIGHT_TEMPLATE)
            st.plotly_chart(fig, use_container_width=True)
        
        with c2:
            # Show UMAP if exists
            umap_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml", "outputs", "umap_projection.csv")
            if os.path.exists(umap_path):
                umap_df = pd.read_csv(umap_path)
                fig_umap = px.scatter(umap_df, x="umap_x", y="umap_y", color="cluster_label", hover_name="ward_name",
                                      title="UMAP Projection", color_discrete_sequence=THEME_COLORS)
                fig_umap.update_traces(marker=dict(size=9, line=dict(width=1.5, color='#FFFFFF'), opacity=0.9))
                fig_umap.update_layout(template=LIGHT_TEMPLATE)
                st.plotly_chart(fig_umap, use_container_width=True)
            else:
                st.info("UMAP projection data not yet available.")
    else:
        st.warning("Cluster data not available.")

# ---- 9. Data Explorer ----
elif page == "📊  Data Explorer":
    st.title("Data Explorer")
    page_subtitle("Filter, sort, and export ward-level liveability data")
    
    score_min = st.slider("Minimum Composite Score", 0, 100, 0)
    filtered = df_scores_latest[df_scores_latest['composite_score'] >= score_min]
    
    st.markdown(f'<p style="color:#64748B; font-size:0.9rem;">Showing <strong style="color:#6366F1;">{len(filtered)}</strong> of {len(df_scores_latest)} wards</p>', unsafe_allow_html=True)
    st.dataframe(filtered[['ward_name', 'composite_score', 'safety_score', 'aqi_score', 'civic_score', 'transit_score', 'green_score', 'affordability_score']],
                use_container_width=True, hide_index=True)
    
    csv = filtered.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download CSV", csv, "explorer_export.csv", "text/csv")

# ---- 10. Tech Showcase ----
elif page == "⚙️  Tech Showcase":
    st.title("Architecture & Tech Stack")
    page_subtitle("The engineering behind the Liveability Scoring System")
    
    st.markdown("""
    <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; margin-top:8px;">
        <div style="background:linear-gradient(135deg, rgba(99,102,241,0.08), rgba(139,92,246,0.08)); border:1.5px solid rgba(99,102,241,0.15); border-radius:16px; padding:24px; position:relative; overflow:hidden;">
            <div style="font-size:2rem; margin-bottom:8px;">🔄</div>
            <h3 style="margin:0 0 12px 0; font-size:1.1rem; color:#1E293B;">Data Pipeline</h3>
            <ul style="color:#64748B; font-size:0.9rem; padding-left:18px; margin:0;">
                <li><strong>Airflow</strong> orchestration (7 DAGs)</li>
                <li><strong>PostgreSQL + PostGIS</strong> database</li>
                <li><strong>dbt</strong> for transformations</li>
            </ul>
        </div>
        <div style="background:linear-gradient(135deg, rgba(16,185,129,0.08), rgba(20,184,166,0.08)); border:1.5px solid rgba(16,185,129,0.15); border-radius:16px; padding:24px; position:relative; overflow:hidden;">
            <div style="font-size:2rem; margin-bottom:8px;">🧠</div>
            <h3 style="margin:0 0 12px 0; font-size:1.1rem; color:#1E293B;">Machine Learning</h3>
            <ul style="color:#64748B; font-size:0.9rem; padding-left:18px; margin:0;">
                <li><strong>Feature Engineering</strong>: Pandas, Scikit-learn</li>
                <li><strong>Clustering</strong>: KMeans + UMAP</li>
                <li><strong>Prediction</strong>: XGBoost classifier</li>
                <li><strong>Explainability</strong>: SHAP</li>
            </ul>
        </div>
        <div style="background:linear-gradient(135deg, rgba(14,165,233,0.08), rgba(6,182,212,0.08)); border:1.5px solid rgba(14,165,233,0.15); border-radius:16px; padding:24px; position:relative; overflow:hidden;">
            <div style="font-size:2rem; margin-bottom:8px;">🖥️</div>
            <h3 style="margin:0 0 12px 0; font-size:1.1rem; color:#1E293B;">Frontend</h3>
            <ul style="color:#64748B; font-size:0.9rem; padding-left:18px; margin:0;">
                <li><strong>Streamlit</strong> + <strong>Plotly</strong></li>
                <li><strong>Folium</strong> for maps</li>
                <li><strong>FastAPI</strong> backend</li>
            </ul>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ---- 11. Ward Directory ----
elif page == "📋  Ward Directory":
    st.title("Ward Directory")
    page_subtitle("Quick reference for ward IDs and names")
    
    # Get unique ward mapping
    ward_mapping = df_scores[['ward_id', 'ward_name']].drop_duplicates().sort_values('ward_id').reset_index(drop=True)
    
    # Add a search bar to easily filter the dataframe
    search_query = st.text_input("🔎 Search by Ward ID or Name", "")
    if search_query:
        ward_mapping = ward_mapping[
            ward_mapping['ward_name'].str.contains(search_query, case=False, na=False) |
            ward_mapping['ward_id'].astype(str).str.contains(search_query, case=False, na=False)
        ]
    
    st.markdown(f'<p style="color:#64748B; font-size:0.9rem;">Showing <strong style="color:#6366F1;">{len(ward_mapping)}</strong> wards</p>', unsafe_allow_html=True)
    st.dataframe(ward_mapping, use_container_width=True, hide_index=True)
