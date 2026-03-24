

import csv
from scripts.db_utils import get_db_connection

def load_stops(file_path):
    with get_db_connection() as conn:
        cur = conn.cursor()

        with open(file_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)

            count = 0
            for row in reader:
                stop_id = row["stop_id"]
                stop_name = row["stop_name"]
                lat = float(row["stop_lat"])
                lon = float(row["stop_lon"])

                cur.execute("""
                    INSERT INTO raw.transit_stops (
                        stop_id, stop_name, stop_lat, stop_lon, geom
                    )
                    VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                    ON CONFLICT (stop_id) DO NOTHING;
                """, (stop_id, stop_name, lat, lon, lon, lat))

                count += 1

        conn.commit()
        print(f"✅ {count} stops inserted!")

if __name__ == "__main__":
    load_stops("data/gtfs/bmtc/in-karnataka-bangalore-metropolitan-transport-corporation-bmtc-gtfs-2013-1/stops.txt")