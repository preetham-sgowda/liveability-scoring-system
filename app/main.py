"""
app/main.py — FastAPI application for Liveability Scoring System.
"""

from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis
import json
import logging
from typing import List, Optional
from datetime import datetime

from scripts.db_utils import get_db_connection

app = FastAPI(title="Liveability Scoring API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

redis = None

@app.on_event("startup")
async def startup():
    global redis
    import os
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = os.getenv("REDIS_PORT", "6379")
    redis = await aioredis.from_url(f"redis://{redis_host}:{redis_port}", decode_responses=True)

@app.get("/scores/{city}")
async def get_city_scores(city: str):
    """Get latest liveability scores for all wards in a city."""
    # Check cache
    cache_key = f"scores:{city.lower()}"
    cached = await redis.get(cache_key)
    if cached:
        return json.loads(cached)

    # Database query
    with get_db_connection() as conn:
        import pandas as pd
        query = """
            SELECT * FROM marts.liveability_scores 
            WHERE city = %s 
            ORDER BY composite_score DESC
        """
        df = pd.read_sql(query, conn, params=(city.title(),))
        results = df.to_dict(orient="records")
        
    # Cache for 1 hour
    await redis.setex(cache_key, 3600, json.dumps(results, default=str))
    return results

@app.get("/wards/{ward_id}/history")
async def get_ward_history(ward_id: int):
    """Get historical trend for a specific ward."""
    with get_db_connection() as conn:
        import pandas as pd
        query = "SELECT * FROM marts.liveability_scores WHERE ward_id = %s ORDER BY year, month"
        df = pd.read_sql(query, conn, params=(ward_id,))
@app.get("/wards/{ward_id}/scores")
async def get_ward_scores(ward_id: int):
    """All year scores for a ward."""
    with get_db_connection() as conn:
        import pandas as pd
        query = "SELECT * FROM marts.liveability_scores WHERE ward_id = %s ORDER BY year"
        df = pd.read_sql(query, conn, params=(ward_id,))
        return df.to_dict(orient="records")

@app.get("/wards/{ward_id}/decline")
async def get_ward_decline(ward_id: int):
    """Decline prediction + SHAP drivers."""
    with get_db_connection() as conn:
        import pandas as pd
        # Latest prediction
        q_pred = "SELECT * FROM marts.ward_decline_predictions WHERE ward_id = %s ORDER BY year DESC LIMIT 1"
        df_pred = pd.read_sql(q_pred, conn, params=(ward_id,))
        
        # Latest drivers
        q_drivers = "SELECT * FROM marts.ward_shap_drivers WHERE ward_id = %s ORDER BY year DESC LIMIT 1"
        df_drivers = pd.read_sql(q_drivers, conn, params=(ward_id,))
        
        return {
            "prediction": df_pred.to_dict(orient="records")[0] if not df_pred.empty else None,
            "drivers": df_drivers.to_dict(orient="records")[0] if not df_drivers.empty else None
        }

@app.get("/clusters/{city}")
async def get_clusters(city: str):
    """All ward cluster assignments."""
    with get_db_connection() as conn:
        import pandas as pd
        query = """
            SELECT w.ward_id, w.ward_name, c.cluster_id, c.cluster_label, c.year
            FROM marts.ward_clusters c
            JOIN raw.ward_boundaries w ON c.ward_id = w.ward_id
            WHERE w.city = %s
        """
        df = pd.read_sql(query, conn, params=(city.title(),))
        return df.to_dict(orient="records")

@app.get("/compare")
async def compare_wards(ward_a: int, ward_b: int):
    """Side-by-side comparison data."""
    with get_db_connection() as conn:
        import pandas as pd
        query = """
            SELECT * FROM marts.liveability_scores 
            WHERE ward_id IN (%s, %s)
            ORDER BY year DESC
        """
        df = pd.read_sql(query, conn, params=(ward_a, ward_b))
        
        # Group by ward_id and get latest
        latest = df.drop_duplicates(subset=['ward_id'], keep='first')
        return latest.to_dict(orient="records")

@app.get("/alerts/{city}")
async def get_alerts(city: str, threshold: float = 0.6):
    """At-risk wards above threshold."""
    with get_db_connection() as conn:
        import pandas as pd
        query = """
            SELECT p.ward_id, w.ward_name, p.decline_probability, p.year,
                   d.driver_1_feature, d.driver_2_feature
            FROM marts.ward_decline_predictions p
            JOIN raw.ward_boundaries w ON p.ward_id = w.ward_id
            LEFT JOIN marts.ward_shap_drivers d ON p.ward_id = d.ward_id AND p.year = d.year
            WHERE w.city = %s AND p.decline_probability >= %s
            ORDER BY p.decline_probability DESC
        """
        df = pd.read_sql(query, conn, params=(city.title(), threshold))
        return df.to_dict(orient="records")

@app.get("/opportunity/{city}")
async def get_opportunity(city: str):
    """High score, low price wards."""
    with get_db_connection() as conn:
        import pandas as pd
        query = """
            WITH latest_scores AS (
                SELECT ward_id, composite_score FROM marts.liveability_scores
                WHERE city = %s
                ORDER BY year DESC
            ),
            latest_features AS (
                SELECT ward_id, ward_name, median_price_sqft 
                FROM marts.mart_ward_features
                WHERE city = %s
                ORDER BY year DESC
            )
            SELECT f.ward_id, f.ward_name, s.composite_score, f.median_price_sqft
            FROM latest_features f
            JOIN latest_scores s ON f.ward_id = s.ward_id
            WHERE s.composite_score > 65 AND f.median_price_sqft > 0
        """
        df = pd.read_sql(query, conn, params=(city.title(), city.title()))
        
        if not df.empty:
            city_median = df['median_price_sqft'].median()
            df = df[df['median_price_sqft'] < city_median]
            df['delta_from_median'] = city_median - df['median_price_sqft']
            df = df.sort_values('delta_from_median', ascending=False)
            
        return df.to_dict(orient="records")

# WebSocket for Real-time AQI
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

@app.websocket("/ws/aqi/{city}")
async def websocket_endpoint(websocket: WebSocket, city: str):
    await manager.connect(websocket)
    try:
        # Send latest AQI on connect
        with get_db_connection() as conn:
            import pandas as pd
            df = pd.read_sql("SELECT * FROM raw.cpcb_aqi WHERE city = %s ORDER BY date DESC LIMIT 10", conn, params=(city.title(),))
            await websocket.send_json(df.to_dict(orient="records"))
            
        while True:
            data = await websocket.receive_text()
            # Handle client heartbeats or messages if needed
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
