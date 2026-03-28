# ADR-002: DuckDB as the Serving Layer Query Engine

**Status:** Accepted  
**Date:** 2026-03-25  
**Author:** Khairunnisa Maharani

## Context
The FastAPI backend requires efficient access to the **Gold layer** for analytics endpoints. The project necessitates a database solution capable of querying **Parquet** files directly to eliminate the need for additional ETL processes between storage and serving.

## Decision
Implement **DuckDB** as the primary query engine for the serving layer.

## Consequences
*   **Zero-ETL Workflow:** Enables direct querying of Parquet files from the Gold layer, significantly reducing pipeline complexity.
*   **OLAP Performance:** Provides exceptional performance for analytical queries on datasets under 100GB.
*   **Trade-off:** DuckDB is not designed for OLTP (Online Transaction Processing) workloads and does not maintain a complex persistent state like traditional RDBMS.

## Alternatives Considered
*   **PostgreSQL** — Requires a separate ETL pipeline to load data from Parquet into tables, increasing architectural overhead.
*   **ClickHouse** — Excessive for this scale; introduces high operational complexity for a solo project.
*   **SQLite** — Lacks optimized support for large-scale analytical queries and columnar data formats.