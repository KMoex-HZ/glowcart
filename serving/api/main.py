from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import os
from datetime import datetime
from typing import List, Dict, Any

# --- API Configuration ---
app = FastAPI(
    title="GlowCart Data API",
    description="High-performance analytics API serving Gold layer data via DuckDB & Parquet.",
    version="1.0.0"
)

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Use environment variable or default path
GOLD_PATH = os.getenv("GOLD_PATH", "/root/glowcart/storage/gold")

def query_parquet(path: str, sql_query: str) -> List[Dict[str, Any]]:
    """
    Executes a DuckDB query against a Parquet file and returns results as a list of dictionaries.
    """
    if not os.path.exists(path):
        return []
        
    # Using DuckDB's in-memory connection to query local Parquet files
    conn = duckdb.connect()
    try:
        df = conn.execute(sql_query).fetchdf()
        return df.to_dict(orient="records")
    finally:
        conn.close()

@app.get("/", tags=["General"])
def root():
    """Root endpoint providing service metadata."""
    return {
        "service": "GlowCart Data API",
        "status": "online",
        "docs_url": "/docs",
        "endpoints": [
            "/health",
            "/api/revenue",
            "/api/funnel",
            "/api/top-products",
            "/api/hourly-activity"
        ]
    }

@app.get("/health", tags=["General"])
def health_check():
    """Service health monitoring."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/revenue", tags=["Analytics"])
def get_revenue():
    """Fetch total revenue aggregated by product category."""
    path = f"{GOLD_PATH}/revenue_by_category.parquet"
    query = f"SELECT * FROM read_parquet('{path}') ORDER BY total_revenue DESC"
    data = query_parquet(path, query)
    return {"data": data, "count": len(data)}

@app.get("/api/funnel", tags=["Analytics"])
def get_conversion_funnel():
    """Fetch user conversion funnel metrics (views to payments)."""
    path = f"{GOLD_PATH}/conversion_funnel.parquet"
    query = f"SELECT * FROM read_parquet('{path}')"
    data = query_parquet(path, query)
    return {"data": data, "count": len(data)}

@app.get("/api/top-products", tags=["Analytics"])
def get_top_products():
    """Fetch top-performing products by revenue."""
    path = f"{GOLD_PATH}/top_products.parquet"
    query = f"SELECT * FROM read_parquet('{path}') ORDER BY total_revenue DESC"
    data = query_parquet(path, query)
    return {"data": data, "count": len(data)}

@app.get("/api/hourly-activity", tags=["Analytics"])
def get_hourly_activity():
    """Fetch hourly user activity trends."""
    path = f"{GOLD_PATH}/hourly_activity.parquet"
    query = f"SELECT * FROM read_parquet('{path}') ORDER BY hour"
    data = query_parquet(path, query)
    return {"data": data, "count": len(data)}
