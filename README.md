# GlowCart — E-commerce Data Platform

> End-to-end data engineering project simulating a real-time Indonesian e-commerce analytics platform.

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.3-black)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.1-orange)
![dbt](https://img.shields.io/badge/dbt-1.11-red)
![Airflow](https://img.shields.io/badge/Airflow-2.9-teal)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)

---

## Overview

GlowCart simulates a real-time analytics platform for an Indonesian e-commerce app. User events — browsing, add to cart, checkout, payment — are streamed through **Apache Kafka**, processed across a **Medallion architecture (Bronze → Silver → Gold)** using **PySpark** and **dbt**, orchestrated by **Apache Airflow**, and served through a **FastAPI** backend with a live **Chart.js** dashboard.

The entire stack runs locally with **Docker Compose** — no cloud required.

---

## Architecture

![Architecture Diagram](assets/architecture.png)

---

## Dashboard

![Dashboard Preview](assets/dashboard.png)

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Ingestion | Apache Kafka | Real-time event streaming |
| Storage | Parquet + Medallion Architecture | Bronze / Silver / Gold layers |
| Transform | PySpark + dbt | Large-scale aggregations + SQL models |
| Orchestration | Apache Airflow | Pipeline scheduling + monitoring |
| Serving | FastAPI + DuckDB | Analytics API endpoints |
| Visualization | Chart.js | BI dashboard |
| Infrastructure | Docker Compose | Local containerized environment |

---

## Project Structure

```
glowcart/
├── ingestion/
│   ├── kafka/          # Kafka producer & consumer
│   └── scripts/        # Event generators (10,000+ events)
├── storage/
│   ├── bronze/         # Raw data ingestion
│   ├── silver/         # Cleaned & validated data
│   └── gold/           # Business-ready aggregations
├── transform/
│   ├── spark/          # PySpark transformation jobs
│   └── dbt/            # dbt models & data quality tests
├── orchestration/
│   └── dags/           # Airflow DAG definitions
├── serving/
│   ├── api/            # FastAPI analytics endpoints
│   └── dashboard/      # Chart.js business dashboard
├── assets/             # Screenshots & diagrams
└── docker-compose.yml  # Full stack infrastructure
```

---

## Quick Start

**Prerequisites**
- Docker Desktop with WSL2 integration
- Python 3.12+
- Java 21 (for PySpark)

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Activate Python environment
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Generate 10,000 e-commerce events
python3 ingestion/scripts/bulk_generate.py

# 4. Run pipeline
python3 storage/bronze/kafka_to_bronze.py
python3 storage/silver/bronze_to_silver.py
python3 storage/gold/silver_to_gold.py

# 5. Run dbt models
cd transform/dbt && dbt run

# 6. Start API + Dashboard
uvicorn serving.api.main:app --host 0.0.0.0 --port 8000
```

---

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | API info |
| `GET /health` | Health check |
| `GET /api/revenue` | Revenue by product category |
| `GET /api/funnel` | Conversion funnel metrics |
| `GET /api/top-products` | Top products by revenue |
| `GET /api/hourly-activity` | Traffic patterns by hour |

---

## Data Flow

```
Generate Events → Kafka → Bronze → Silver → Gold → FastAPI → Dashboard
                                      ↑
                               PySpark + dbt
                                      ↑
                                   Airflow
```

---

## Author

**Khairunnisa Maharani** — Data Science Student, Institut Teknologi Sumatera (ITERA)
