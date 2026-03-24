# Airflow.Dockerfile
FROM apache/airflow:2.8.1-python3.11

USER root
# Install system dependencies for geospatial libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy and install additional requirements
# We use requirements-airflow.txt (no pins) with Airflow constraints to resolve conflicts
COPY requirements-airflow.txt .
RUN pip install --no-cache-dir \
    "apache-airflow==2.8.1" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt" \
    -r requirements-airflow.txt
