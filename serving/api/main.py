from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import os
from datetime import datetime

app = FastAPI(
    title="GlowCart Data API",
    description="E-commerce analytics API powered by GlowCart Data Platform",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

GOLD_PATH = "/root/glowcart/storage/gold"

def query_parquet(path: str, query: str):
    conn = duckdb.connect()
    result = conn.execute(query).fetchdf()
    conn.close()
    return result.to_dict(orient="records")

@app.get("/")
def root():
    return {
        "message": "GlowCart Data API",
        "version": "1.0.0",
        "endpoints": [
            "/health",
            "/api/revenue",
            "/api/funnel",
            "/api/top-products",
            "/api/hourly-activity"
        ]
    }

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "GlowCart Data API"
    }

@app.get("/api/revenue")
def get_revenue():
    path = f"{GOLD_PATH}/revenue_by_category.parquet"
    data = query_parquet(path, f"SELECT * FROM read_parquet('{path}') ORDER BY total_revenue DESC")
    return {"data": data, "count": len(data)}

@app.get("/api/funnel")
def get_funnel():
    path = f"{GOLD_PATH}/conversion_funnel.parquet"
    data = query_parquet(path, f"SELECT * FROM read_parquet('{path}')")
    return {"data": data, "count": len(data)}

@app.get("/api/top-products")
def get_top_products():
    path = f"{GOLD_PATH}/top_products.parquet"
    data = query_parquet(path, f"SELECT * FROM read_parquet('{path}') ORDER BY total_revenue DESC")
    return {"data": data, "count": len(data)}

@app.get("/api/hourly-activity")
def get_hourly_activity():
    path = f"{GOLD_PATH}/hourly_activity.parquet"
    data = query_parquet(path, f"SELECT * FROM read_parquet('{path}') ORDER BY hour")
    return {"data": data, "count": len(data)}
