"""
geo/utils.py — Shared geospatial utilities.
"""

CITY_BBOXES = {
    "Bengaluru": {"west": 77.35, "south": 12.73, "east": 77.85, "north": 13.18},
    "Mumbai": {"west": 72.77, "south": 18.89, "east": 72.98, "north": 19.27},
    "Delhi": {"west": 76.84, "south": 28.41, "east": 77.35, "north": 28.88},
}

def is_within_city(lat, lon, city):
    """Check if a coordinate is within a city's bounding box."""
    bbox = CITY_BBOXES.get(city)
    if not bbox:
        return False
    return (bbox["south"] <= lat <= bbox["north"] and 
            bbox["west"] <= lon <= bbox["east"])
