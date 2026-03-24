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
