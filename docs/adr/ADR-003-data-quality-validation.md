# ADR-003: Custom Pandas Validation for Data Quality

**Status:** Accepted  
**Date:** 2025-03-27  
**Author:** Khairunnisa Maharani

## Context
The data pipeline requires documented data quality checks to serve as a **gating mechanism**. Data must be strictly prohibited from entering the **Silver layer** if validation criteria are not met.

## Decision
Implement a custom validation framework using **pandas** during the Silver layer processing. While **Great Expectations (GE)** was initially considered, the project has migrated to a lightweight custom validation approach to maintain agility.

## Consequences
*   **Early Detection:** Data quality issues are identified and intercepted before they propagate to the Silver layer.
*   **Maintainability:** Improved code readability with zero heavy dependency overhead.
*   **Trade-off:** Does not provide automated HTML reporting or the extensive profiling features inherent to Great Expectations.

## Alternatives Considered
*   **Great Expectations** — A powerful industry standard, but the v1.x release introduced significant breaking API changes (e.g., deprecation of `from_pandas()`), creating excessive management overhead for a solo project.
*   **dbt tests** — Excellent for SQL-level validation, but cannot cover the initial ingestion and transformation layers before the data reaches the warehouse.
*   **Custom assertions** — Similar to the chosen path, but lacked a consistent structure and reusability across different pipeline stages.