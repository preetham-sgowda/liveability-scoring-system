# LSS Data Directory Structure

Please place your raw source files in these directories before triggering the Airflow DAGs.

### ncrb/
Place NCRB "Crime in India" PDFs here. (e.g., `Crime_in_India_2022.pdf`)

### census/
Place the ward-level census CSV here. (e.g., `census_2011_wards.csv`)

### gtfs/bmtc/
Place the BMTC GTFS feed files here (unzipped). Must include `stops.txt`.

### wards/
Place the `wards.geojson` file here for spatial joins and NDVI aggregation.
