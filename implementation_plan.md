# LSS — Phases 3, 4 & 5 Implementation Plan

Build the remaining Feature Engineering, ML, and Dashboard layers for the Liveability Scoring System (Bengaluru, 198 BBMP wards).

## User Review Required

> [!IMPORTANT]
> **Dimension weight changes**: The PRD specifies 6 dimensions with weights (Safety 25%, Environment 20%, Civic Health 20%, Connectivity 15%, Socioeconomic 10%, Infrastructure 10%) but the current `scoring.py` uses a different 6-dimension scheme (safety 20%, aqi 20%, civic 15%, transit 20%, green 15%, affordability 10%). This plan remaps to the PRD weights.

> [!IMPORTANT]  
> **No running PostgreSQL database**: All code will be written to work against the existing DB schema, but verification will be limited to import/syntax checks since there's no live DB in this environment. ML model training & Streamlit page rendering require actual data.

> [!WARNING]
> **Synthetic data fallback**: Since there's no live DB, ML and dashboard code includes CSV fallback (`data/processed/ward_features_enriched.csv`) so the Streamlit app can demo without PostgreSQL.

---

## Proposed Changes

### Dependencies

#### [MODIFY] [requirements.txt](file:///c:/Desktop/liveability-scoring-system/requirements.txt)
Add missing ML, visualization, and dashboard dependencies:
`xgboost`, `optuna`, `shap`, `umap-learn`, `libpysal`, `esda`, `streamlit`, `folium`, `streamlit-folium`, `plotly`, `matplotlib`, `seaborn`

---

### SQL Schema

#### [NEW] [007_ml_tables.sql](file:///c:/Desktop/liveability-scoring-system/sql/007_ml_tables.sql)
Create 3 new mart tables for ML outputs:
- `marts.ward_clusters` (ward_id, year, cluster_id, cluster_label)
- `marts.ward_decline_predictions` (ward_id, year, decline_probability, decline_predicted, model_version)
- `marts.ward_shap_drivers` (ward_id, year, driver_1/2/3 feature + value pairs)

Also adds derived columns to `marts.mart_ward_features` via `ALTER TABLE` (crime_trend_3y, aqi_trend_3y, ndvi_trend_3y, complaint_backlog_pct, transit_walkability, cluster_id, cluster_label).

---

### Feature Engineering (Phase 3)

#### [NEW] [ml/__init__.py](file:///c:/Desktop/liveability-scoring-system/ml/__init__.py)
Empty init for the `ml` package.

#### [NEW] [ml/feature_engineering.py](file:///c:/Desktop/liveability-scoring-system/ml/feature_engineering.py)
- Load `marts.mart_ward_features` via `db_utils.get_db_connection()` (with CSV fallback)
- KNN imputation (k=5) for features with <30% missingness
- Forward-fill census data across years
- Compute derived features:
  - `crime_trend_3y`, `aqi_trend_3y`, `ndvi_trend_3y` — 3-year linear regression slopes per ward
  - `complaint_backlog_pct` — pending / total complaints
  - `transit_walkability` — composite of bus stops + inverse metro proximity
  - `yoy_price_change_3y_avg` — 3-year rolling average of price_change_yoy
- Save to `data/processed/ward_features_enriched.csv` and optionally upsert to DB

#### [MODIFY] [scripts/scoring.py](file:///c:/Desktop/liveability-scoring-system/scripts/scoring.py)
- Remap dimensions to PRD's 6 categories: Safety (25%), Environment (20%), Civic Health (20%), Connectivity (15%), Socioeconomic (10%), Infrastructure (10%)
- Add `transit_score` computation (normalize bus_stops + inverse metro_proximity)
- Add validation: Pearson correlation of composite_score vs median_price_sqft (target r > 0.5)
- Improve inversion logic per dimension

#### [NEW] [notebooks/eda_and_feature_engineering.ipynb](file:///c:/Desktop/liveability-scoring-system/notebooks/eda_and_feature_engineering.ipynb)
Jupyter notebook with:
- Distribution plots for all 35+ features (histograms + KDE)
- Missingness heatmap (seaborn)
- Correlation matrix heatmap
- Moran's I spatial autocorrelation (libpysal + esda)
- Moran's I scatter plot + cluster identification (HH, LL, HL, LH)

---

### Machine Learning (Phase 4)

#### [NEW] [ml/clustering.py](file:///c:/Desktop/liveability-scoring-system/ml/clustering.py)
- Load enriched features (latest year)
- StandardScaler on 7 clustering features
- Grid search k ∈ [3, 8] by elbow + silhouette (target > 0.35)
- Fit KMeans with best k
- Name clusters by mean feature profiles (Urban Core, Green Suburbs, Distressed, etc.)
- UMAP 2D projection → save to `ml/outputs/umap_projection.csv`
- Save cluster labels to `marts.ward_clusters` and `data/processed/ward_clusters.csv`

#### [NEW] [ml/decline_predictor.py](file:///c:/Desktop/liveability-scoring-system/ml/decline_predictor.py)
- Label generation: decline = 1 if next-year score drops ≥ 10 points
- Temporal split: train 2018–2021, test 2022–2023, validate 2024
- XGBClassifier with Optuna (50 trials, optimize ROC-AUC)
- Targets: ROC-AUC > 0.75, Precision > 0.60, Recall > 0.50
- Save model: `ml/models/decline_predictor_v1.joblib`
- Save predictions to `marts.ward_decline_predictions` and CSV

#### [NEW] [ml/shap_explainability.py](file:///c:/Desktop/liveability-scoring-system/ml/shap_explainability.py)
- `shap.TreeExplainer` on trained XGBoost model
- Global summary plot → `ml/outputs/shap_summary.png`
- Per-ward top-3 SHAP drivers extraction
- `generate_waterfall(ward_id, year)` → matplotlib figure
- Save drivers to `marts.ward_shap_drivers` and CSV

---

### Dashboard & API (Phase 5)

#### [MODIFY] [app/main.py](file:///c:/Desktop/liveability-scoring-system/app/main.py)
Add 6 new FastAPI endpoints:
- `GET /wards/{ward_id}/scores` — all year scores for a ward
- `GET /wards/{ward_id}/decline` — decline prediction + SHAP drivers
- `GET /clusters/{city}` — ward cluster assignments
- `GET /compare` — side-by-side ward comparison
- `GET /alerts/{city}` — at-risk wards above threshold
- `GET /opportunity/{city}` — high score, low price wards

#### [NEW] [app/streamlit_app.py](file:///c:/Desktop/liveability-scoring-system/app/streamlit_app.py)
10-page Streamlit app with sidebar navigation:
1. **Landing Map** — Folium choropleth colored by composite score, search, hover tooltips
2. **Ward Details** — metric widget, radar chart (Plotly), raw metrics table, year slider
3. **Compare Wards** — side-by-side dimension bars, winner badges
4. **Declining Ward Alerts** — top-10 at-risk, SHAP waterfall expand
5. **Trends Dashboard** — multi-ward line charts (Plotly), dimension sub-charts
6. **Opportunity Wards** — scatter plot (score vs price), anomaly highlighting
7. **City Overview** — KPI cards, score histogram, top/bottom-5 bars, cluster pie
8. **Ward Typology** — cluster choropleth, profile cards, UMAP scatter
9. **Data Explorer** — dynamic filter sliders, live map + table, CSV export
10. **Tech Showcase** — static markdown with pipeline diagram, stack table, team info

---

## Verification Plan

### Automated Tests

Since there is no live PostgreSQL in this environment, verification is scoped to:

```bash
# 1. Syntax/import check for all new Python files
python -c "import ml.feature_engineering; import ml.clustering; import ml.decline_predictor; import ml.shap_explainability"

# 2. Check FastAPI app loads
python -c "from app.main import app; print(app.routes)"

# 3. Check Streamlit app file parses
python -c "import ast; ast.parse(open('app/streamlit_app.py').read()); print('OK')"

# 4. Existing tests still pass
pytest tests/ -v
```

### Manual Verification

1. **Streamlit smoke test**: Run `streamlit run app/streamlit_app.py` — verify sidebar loads with 10 page options, no crash on startup (pages will show empty state without DB data)
2. **FastAPI swagger**: Run `uvicorn app.main:app --reload` — verify Swagger docs at `/docs` show all 8 endpoints
3. **ML pipeline** (requires DB): Run `python ml/feature_engineering.py`, then `python ml/clustering.py`, then `python ml/decline_predictor.py`, then `python ml/shap_explainability.py` — check outputs in `data/processed/` and `ml/outputs/`
