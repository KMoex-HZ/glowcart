# GlowCart — Production-Inspired E-commerce Data Platform

> End-to-end data engineering platform simulating a real-time Indonesian e-commerce analytics pipeline — Built with production engineering practices: DLQ, idempotency, data quality gating, and ADRs

![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.5-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.1-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?style=flat-square&logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/Tests-9%20passed-2ea44f?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)

---

## Overview

GlowCart simulates a production-grade analytics platform for an Indonesian e-commerce app. User events — browsing, add to cart, checkout, payment — are streamed through **Apache Kafka**, processed across a **Medallion architecture (Bronze → Silver → Gold)** using **PySpark** and **dbt**, orchestrated by **Apache Airflow**, and served through a **FastAPI** backend with a live **Chart.js** dashboard.

This project goes beyond a tutorial pipeline. It is engineered to handle real-world failure scenarios: corrupt events are captured in a **Dead Letter Queue**, every pipeline step is **idempotent** (safe to rerun), data quality is enforced as a **gating mechanism** before each layer promotion, and all architectural decisions are documented in **ADRs**.

The entire stack runs locally with **Docker Compose** — no cloud required.

---

## Architecture Overview

```
Generate Events → Kafka → Bronze → Silver → Gold → FastAPI → Dashboard
                    ↓        ↓        ↓
                   DLQ   Idempotent  Quality
                         Check       Gate
                                      ↑
                               PySpark + dbt
                                      ↑
                                   Airflow
```

![Architecture Diagram](assets/architecture.png)

---

## Dashboard

![Dashboard Preview](assets/dashboard.png)

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Apache Kafka | Real-time event streaming with Dead Letter Queue |
| Storage | Parquet + Medallion Architecture | Bronze / Silver / Gold layers |
| Transform | PySpark + dbt | Large-scale aggregations + SQL models |
| Orchestration | Apache Airflow | Pipeline scheduling + SLA monitoring |
| Serving | FastAPI + DuckDB | Analytics API — zero ETL from Gold layer |
| Visualization | Chart.js | Live BI dashboard |
| Quality | Custom Pandas Validation | Gating mechanism at Silver layer with HTML reports |
| Infrastructure | Docker Compose | Local containerized environment |

---

## Production Engineering Features

What separates this from a standard tutorial project:

**Dead Letter Queue (DLQ)**
Corrupt or malformed Kafka events are never silently dropped or crash the pipeline. They are routed to a dedicated `glowcart-events.dlq` topic with the original payload, error reason, and timestamp — enabling replay and audit. See [ADR-004](docs/adr/ADR-004-dead-letter-queue.md).

**Idempotency**
Every pipeline step (Bronze, Silver, Gold) checks for existing output before processing. Rerunning the pipeline at any point produces identical results with zero data duplication — safe for Airflow retries. See [ADR-005](docs/adr/ADR-005-idempotency-design.md).

**Data Quality as a Gating Mechanism**
The Silver layer runs custom pandas validation checks before writing any data. If checks fail — null IDs, invalid event types, out-of-range amounts — the pipeline halts with a non-zero exit code. Airflow captures this as a task failure. Data does not proceed downstream. HTML validation reports are saved to `docs/quality-reports/`. See [ADR-003](docs/adr/ADR-003-data-quality-validation.md).

**Structured Logging**
All `print()` statements replaced with a centralized logger (`utils/logger.py`) producing structured, timestamped, leveled log output — compatible with any log aggregation system.

**Unit Tests**
9 tests covering event validation logic and Silver layer data quality rules. Run with `pytest tests/ -v`.

**Architecture Decision Records (ADRs)**
Every major technical choice is documented with context, decision, consequences, and alternatives considered — the format used by senior engineers at production companies. See [`docs/adr/`](docs/adr/).

---

## Idempotency Testing

Every pipeline stage (Bronze → Silver → Gold) is safe to rerun multiple times. Rerunning produces identical output with zero data duplication.

**How it works:**
- Bronze: checks if `events.parquet` already exists for today's date before consuming from Kafka
- Silver: checks if `events.parquet` already exists before running transformations
- Gold: checks if `.done_{date}` marker file exists before rebuilding aggregations

**Proof — running the pipeline twice:**
```
# First run
python3 -m storage.bronze.kafka_to_bronze
→ INFO | Successfully ingested 10000 events

# Second run (same day)
python3 -m storage.bronze.kafka_to_bronze
→ INFO | Skipping: Bronze layer for 20260329 already exists

# Output is identical — no duplicates created
```

This makes the pipeline safe for Airflow retries — if a task fails midway and Airflow retries it, data integrity is guaranteed.

---

## Design Decisions

All major engineering decisions are documented under [`docs/adr/`](docs/adr/):

| ADR | Decision | Status |
|---|---|---|
| [ADR-001](docs/adr/ADR-001-parquet-storage-format.md) | Parquet as storage format across all layers | Accepted |
| [ADR-002](docs/adr/ADR-002-duckdb-serving-layer.md) | DuckDB for serving layer — zero ETL from Gold | Accepted |
| [ADR-003](docs/adr/ADR-003-data-quality-validation.md) | Custom pandas validation as Silver gating mechanism | Accepted |
| [ADR-004](docs/adr/ADR-004-dead-letter-queue.md) | Dead Letter Queue for Kafka error handling | Accepted |
| [ADR-005](docs/adr/ADR-005-idempotency-design.md) | File-based idempotency for all pipeline steps | Accepted |
| [ADR-006](docs/adr/ADR-006-docker-compose.md) | Docker Compose over Kubernetes for local dev | Accepted |
| [ADR-007](docs/adr/ADR-007-python-3-12.md) | Python 3.12 — performance + latest typing features | Accepted |

---

## What I Would Do Differently at Scale

This project runs entirely on Docker Compose for local development. At production scale, here's how the architecture would evolve:

**Orchestration:** Docker Compose works for local demos. In production, I'd migrate to Kubernetes with Helm charts to enable horizontal scaling of Kafka consumers independently from other services.

**Storage:** Local Parquet files would be replaced with S3 or GCS backed by Delta Lake — adding ACID transactions, time travel, and schema enforcement that local Parquet cannot provide.

**Serving Layer:** DuckDB is a great fit for datasets under 100GB. Beyond that, I'd evaluate ClickHouse for its OLAP performance, or BigQuery for serverless scalability with pay-per-query pricing.

**Data Quality:** Custom pandas validation works well for a single pipeline. At scale, I'd migrate to Great Expectations or Soda Core — both generate shareable HTML reports and integrate natively with Airflow, making quality checks visible to non-engineers.

---

## Known Limitations

Being transparent about trade-offs is part of good engineering:

- **No horizontal scaling**: Docker Compose runs all services on a single machine. Kafka consumer throughput cannot be scaled by adding replicas.
- **Local storage only**: Parquet files are written to local disk. No replication or redundancy — data loss on disk failure.
- **DuckDB is single-process**: Not suitable for concurrent writes or multi-user analytics at scale.
- **Simulated dirty data**: Anomalies are injected synthetically. Real-world data quality issues are often more unpredictable and domain-specific.
- **No schema registry**: Schema evolution is handled manually. A production system would use Confluent Schema Registry or AWS Glue Schema Registry.

---

## Project Structure

```
glowcart/
├── ingestion/
│   ├── kafka/              # Kafka producer, consumer, Dead Letter Queue
│   └── scripts/            # Event generators (10,000+ events)
├── storage/
│   ├── bronze/             # Raw ingestion from Kafka — idempotent
│   ├── silver/             # Cleaned, validated data — quality gated
│   └── gold/               # Business-ready aggregations — idempotent
├── transform/
│   ├── spark/              # PySpark transformation jobs
│   └── dbt/                # dbt models & data quality tests
├── orchestration/
│   └── dags/               # Airflow DAG definitions
├── serving/
│   ├── api/                # FastAPI analytics endpoints
│   └── dashboard/          # Chart.js live dashboard
├── tests/                  # Unit tests (pytest) — 9 tests, all passing
├── utils/
│   └── logger.py           # Centralized structured logger
├── docs/
│   ├── adr/                # Architecture Decision Records (ADR-001 to ADR-007)
│   ├── quality-reports/    # Great Expectations HTML validation reports
│   └── runbook/            # Operational runbooks for pipeline failures
├── assets/                 # Screenshots & diagrams
└── docker-compose.yml      # Full stack infrastructure
```

---

## Quick Start

**Prerequisites**
- Docker Desktop with WSL2 integration
- Python 3.12+
- Java 21 (for PySpark)

```bash
# 1. Clone and enter project
git clone https://github.com/KMoex-HZ/glowcart.git
cd glowcart

# 2. Start infrastructure
docker compose up -d

# 3. Activate Python environment
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 4. Generate 10,000 e-commerce events
python3 ingestion/scripts/bulk_generate.py

# 5. Run pipeline (each step is idempotent — safe to rerun)
python3 storage/bronze/kafka_to_bronze.py
python3 storage/silver/bronze_to_silver.py
python3 storage/gold/silver_to_gold.py

# 6. Run dbt models
cd transform/dbt && dbt run

# 7. Start API + Dashboard
uvicorn serving.api.main:app --host 0.0.0.0 --port 8000

# 8. Run tests
pytest tests/ -v
```

---

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | API info |
| `GET /health` | Health check |
| `GET /api/revenue` | Revenue by product category |
| `GET /api/funnel` | Conversion funnel metrics |
| `GET /api/top-products` | Top 5 products by revenue |
| `GET /api/hourly-activity` | Traffic patterns by hour |

---

## Operational Runbooks

Production-grade pipelines need documented recovery procedures. See [`docs/runbook/`](docs/runbook/):

| Runbook | Covers |
|---|---|
| [PIPELINE_FAILURE.md](docs/runbook/PIPELINE_FAILURE.md) | Steps if Bronze ingestion or any pipeline stage crashes |
| [DATA_QUALITY_FAILURE.md](docs/runbook/DATA_QUALITY_FAILURE.md) | Steps if Great Expectations validation fails at Silver |

---

## Cloud Cost Estimate

See [`docs/COST_ANALYSIS.md`](docs/COST_ANALYSIS.md) for a full breakdown of estimated AWS costs at current and scaled data volumes, including optimization strategies and bottleneck analysis.

---

## Author

**Khairunnisa Maharani**  
Data Science — Institut Teknologi Sumatera (ITERA)  
[GitHub](https://github.com/KMoex-HZ) · [LinkedIn](https://www.linkedin.com/in/khnrni/)