import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("CPCB_API_KEY")
url = "https://app.cpcbccr.com/ccr_docs/ccr_doc/api/station_data"
params = {
    "station_id": "site_5029",
    "date": "2026-03-24",
    "format": "json"
}

headers = {
    "Authorization": f"Bearer {API_KEY}"
}

print(f"Testing API with URL: {url}")
print(f"Params: {params}")
print(f"Headers: Authorization: Bearer {API_KEY[:10]}...")

response = requests.get(url, params=params, headers=headers)
print(f"Status: {response.status_code}")
print(f"Response: {response.text[:500]}")