# ADR-001: Parquet as Storage Format

**Status:** Accepted  
**Date:** 2026-03-25  
**Author:** Khairunnisa Maharani

## Context
The data pipeline is required to store millions of e-commerce events. The storage format must be optimized for analytical queries and provide seamless interoperability between **Spark** and **DuckDB**.

## Decision
Adopt **Apache Parquet** with **Snappy compression** for all data layers (Bronze, Silver, and Gold).

## Consequences
*   **Performance:** Analytical queries are 10–100x faster compared to CSV.
*   **Optimization:** Enables native **column pruning** and **predicate pushdown**.
*   **Trade-off:** Files are not human-readable in plain text editors or Excel; requires specialized tools like Spark, DuckDB, or pandas for inspection.

## Alternatives Considered
*   **CSV** — Human-readable and ubiquitous, but highly inefficient for large-scale analytical processing.
*   **JSON** — Provides schema flexibility but incurs significant storage overhead and slower I/O.
*   **Delta Lake / Iceberg** — Offers advanced features (ACID transactions, time travel), but introduces unnecessary management overhead for the current scope of a solo project.