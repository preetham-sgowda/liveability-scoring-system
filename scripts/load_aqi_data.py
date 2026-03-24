import requests
import os
from datetime import datetime
from dotenv import load_dotenv
from scripts.db_utils import get_db_connection
from scripts.api_clients.cpcb_aqi_client import CpcbAqiClient

# Load environment variables from .env
load_dotenv()


def load_aqi_data():
    API_KEY = os.getenv("CPCB_API_KEY")

    if not API_KEY:
        print("❌ CPCB_API_KEY environment variable not set")
        return

    # Debug: Check environment variables
    print(f"🔍 CPCB_API_BASE_URL env var: {os.getenv('CPCB_API_BASE_URL')}")
    print(f"🔍 Default base URL would be: https://app.cpcbccr.com/ccr_docs/ccr_doc/api")

    # Use the proper CPCB client instead of direct API calls
    client = CpcbAqiClient(api_key=API_KEY)
    
    print(f"🔗 Using CPCB API base URL: {client.base_url}")

    # Fetch latest readings for Bengaluru
    print("📡 Fetching latest AQI data from CPCB...")
    readings = client.fetch_latest_readings("Bengaluru")

    if not readings:
        print("⚠️ No AQI readings fetched from CPCB API")
        return

    print(f"📊 Fetched {len(readings)} station readings")

    count = 0
    skipped = 0

    with get_db_connection() as conn:
        cur = conn.cursor()

        for reading in readings:
            try:
                # Insert into the correct table with all available data
                cur.execute("""
                    INSERT INTO raw.cpcb_aqi (
                        station_id, station_name, city, latitude, longitude,
                        date, pm25, pm10, no2, so2, co, o3, aqi, prominent_pollutant
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (station_id, date) DO NOTHING
                """, (
                    reading.get("station_id"),
                    reading.get("station_name"),
                    reading.get("city"),
                    reading.get("latitude"),
                    reading.get("longitude"),
                    reading.get("date"),
                    reading.get("pm25"),
                    reading.get("pm10"),
                    reading.get("no2"),
                    reading.get("so2"),
                    reading.get("co"),
                    reading.get("o3"),
                    reading.get("aqi"),
                    reading.get("prominent_pollutant")
                ))

                count += 1

            except Exception as e:
                skipped += 1
                if skipped <= 3:
                    print(f"  ❌ Error inserting reading: {e}")

        conn.commit()

    if count > 0:
        print(f"✅ AQI loaded: {count} rows")
    else:
        print(f"⚠️ AQI loaded: {count} rows (skipped {skipped})")


if __name__ == "__main__":
    load_aqi_data()