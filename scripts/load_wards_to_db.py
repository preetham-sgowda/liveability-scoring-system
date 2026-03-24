import json
import os
import argparse
from scripts.db_utils import get_db_connection

def load_wards(file_path, city_name):
    # DB connection using shared utility (uses environment variables)
    # For local execution, set POSTGRES_HOST=localhost and POSTGRES_PORT=5433
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("POSTGRES_PORT", "5433")

    with get_db_connection() as conn:
        cur = conn.cursor()

        # Load GeoJSON
        print(f"Loading {file_path} for city: {city_name}...")
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return

        with open(file_path, encoding="utf-8") as f:
            geojson = json.load(f)

        count = 0
        for feature in geojson["features"]:
            props = feature["properties"]
            geometry = json.dumps(feature["geometry"])

            # Extract properties from Bengaluru GeoJSON structure
            # Priority: proposed_ward_name_en (has number), then name_en
            ward_name_full = props.get("proposed_ward_name_en") or props.get("name_en") or "unknown"
            
            # Ward number: extract from full name or use id
            raw_ward_no = props.get("id") or 0
            try:
                ward_number = int(float(raw_ward_no))
            except (ValueError, TypeError):
                ward_number = 0
                
            zone_name = "unknown"  # GeoJSON doesn't have zone info
            
            # Area in sq km
            area_sqkm = props.get("ward_area") or 0
            try:
                area_sqkm = float(area_sqkm)
            except (ValueError, TypeError):
                area_sqkm = 0

            cur.execute(
                """
                SELECT raw.upsert_ward_boundary(%s, %s, %s, %s, %s, %s)
                """,
                (ward_name_full, ward_number, city_name, zone_name, area_sqkm, geometry)
            )
            count += 1

        print(f"✅ {count} wards for {city_name} inserted successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Ward GeoJSON to Database")
    parser.add_argument("--file", default="data/wards/bengaluru_wards.geojson", help="Path to GeoJSON file")
    parser.add_argument("--city", default="Bengaluru", help="City name (e.g. Bengaluru, Mumbai, Delhi)")
    
    args = parser.parse_args()
    
    load_wards(args.file, args.city)