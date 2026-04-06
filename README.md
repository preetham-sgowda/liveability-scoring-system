# Liveability Scoring System (LSS)

A data-driven platform that analyzes crime, environment, infrastructure, and transit data to compute ward-level liveability scores for Bengaluru (198 BBMP wards) and predict future decline using machine learning and geospatial analytics.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES (7)                            │
│  NCRB PDFs │ BBMP Sahaaya │ CPCB API │ Census │ GTFS │ GEE │ Property │
└──────┬──────┬──────────────┬──────────┬────────┬──────┬─────┬──┘
       │      │              │          │        │      │     │
       ▼      ▼              ▼          ▼        ▼      ▼     ▼
┌─────────────────────────────────────────────────────────────────┐
│              APACHE AIRFLOW (7 DAGs)                            │
│  TaskFlow API │ Retry Logic │ XCom │ Pipeline Audit Logging     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│           POSTGRESQL 15 + PostGIS 3.3                           │
│  ┌──────────┐   ┌───────────┐   ┌──────────────────────┐       │
│  │ raw.*    │──▶│ staging.* │──▶│ marts.mart_ward_     │       │
│  │ (8 tbls) │   │ (7 tbls)  │   │      features        │       │
│  └──────────┘   └───────────┘   └──────────────────────┘       │
│       ▲              ▲                    │                     │
│       │         dbt transforms            │                     │
└───────┴───────────────────────────────────┼─────────────────────┘
                                            │
                                            ▼
                              ┌──────────────────────┐
                              │  ML Layer (Preetham) │
                              │  KMeans │ XGBoost    │
                              │  SHAP Explainability │
                              └──────────────────────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.8 (TaskFlow API) |
| Database | PostgreSQL 15 + PostGIS 3.3 |
| Transformations | dbt-postgres 1.7 |
| PDF Parsing | pdfplumber |
| Web Scraping | Scrapy 2.11 |
| Geospatial | GeoFandas, rasterio, rasterstats, H3 |
| Remote Sensing | Google Earth Engine API |
| Infrastructure | PostgreSQL + PostGIS (managed separately) |

## Quick Start

```bash
# 1. Clone and setup
git clone <repo-url>
cd liveability-scoring-system
cp .env.example .env  # Edit with your credentials

# 2. Start infrastructure
Start PostgreSQL 15 + PostGIS and Airflow using your preferred method (native install, cloud service, or system service). Ensure your database is reachable and configure `.env` accordingly.

# 3. (Optional) Access Airflow UI
Open the Airflow UI on your Airflow host/port (e.g., http://<airflow-host>:8080) and sign in with your Airflow credentials.

# 4. Run dbt transformations
cd dbt_lss
dbt run
dbt test
```

## Project Structure

```
├── docker-compose.yml          # PostgreSQL + PostGIS, Airflow
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable template
│
├── sql/                        # Database migrations
│   ├── 001_extensions.sql      # PostGIS, uuid-ossp
│   ├── 002_raw_schema.sql      # 8 raw tables + pipeline_runs
│   ├── 003_staging_schema.sql  # 7 staging tables
│   ├── 004_marts_schema.sql    # mart_ward_features + view
│   └── 005_seed_wards.sql      # Ward boundary upsert function
│
├── dags/                       # Airflow DAGs (7 pipelines)
│   ├── dag_ncrb_crime.py       # NCRB PDF ingestion
│   ├── dag_bbmp_sahaaya.py     # BBMP complaints (weekly)
│   ├── dag_cpcb_aqi.py         # CPCB AQI readings (daily)
│   ├── dag_census.py           # Census CSV (manual)
│   ├── dag_gtfs_transit.py     # GTFS transit (monthly)
│   ├── dag_ndvi.py             # GEE NDVI (monthly)
│   └── dag_property_prices.py  # Property prices (weekly)
│
├── scripts/                    # Ingestion logic
│   ├── db_utils.py             # DB connection, bulk insert, audit log
│   ├── parsers/
│   │   └── ncrb_pdf_parser.py  # pdfplumber crime PDF parser
│   ├── api_clients/
│   │   └── cpcb_aqi_client.py  # CPCB REST API with retry
│   ├── scrapers/
│   │   ├── bbmp_sahaaya/       # Full Scrapy project
│   │   └── property_scraper.py # MagicBricks/99acres
│   ├── loaders/
│   │   ├── census_loader.py    # Census CSV loader
│   │   └── gtfs_loader.py      # GTFS feed parser
│   └── geo/
│       ├── ndvi_pipeline.py    # GEE Sentinel-2 NDVI
│       └── ward_spatial_utils.py # PostGIS spatial joins
│
├── dbt_lss/                    # dbt project
│   ├── models/
│   │   ├── staging/            # 7 staging models + sources
│   │   └── marts/              # mart_ward_features
│   └── tests/
│
└── tests/                      # Python unit tests
    ├── test_ncrb_parser.py
    └── test_cpcb_client.py
```

## Data Sources

| # | Source | Schedule | Method |
|---|--------|----------|--------|
| 1 | NCRB Crime PDFs | Annual (manual) | pdfplumber |
| 2 | BBMP Sahaaya Complaints | Weekly | Scrapy |
| 3 | CPCB AQI API | Daily | REST + retry |
| 4 | Census of India | One-time | CSV loader |
| 5 | GTFS Transit (BMTC) | Monthly | GTFS parser |
| 6 | Sentinel-2 NDVI | Monthly | GEE + rasterio |
| 7 | Property Prices | Weekly | BeautifulSoup |

## Database Schema

- **`raw.*`** — Exact dumps from all 7 sources + `pipeline_runs` audit table
- **`staging.*`** — Cleaned, typed, PostGIS ward-joined, aggregated by (ward, year)
- **`marts.mart_ward_features`** — Wide table: 198 wards × 7 years, 35+ feature columns

## Pipeline Conventions

All Airflow DAGs follow:
- **TaskFlow API** (`@task` decorators)
- **XCom** for inter-task data passing  
- **Retry**: 3 retries, 5-min delay
- **Idempotent**: delete-before-insert for reprocessing
- **Audit**: every run logged to `raw.pipeline_runs`

## Team

- **Sanjana** — Data Engineering (pipelines, warehouse, dbt)
- **Preetham** — ML & Analytics (EDA, scoring, KMeans, XGBoost, SHAP)
