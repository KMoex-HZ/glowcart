# ADR-005: File-based Idempotency for Pipeline Stages

**Status:** Accepted  
**Date:** 2025-03-27  
**Author:** Khairunnisa Maharani

## Context
The pipeline is orchestrated by **Apache Airflow** on a fixed schedule. To maintain data integrity, the pipeline must ensure **idempotent execution**—meaning if a task is retried or triggered multiple times within the same interval, it must not result in duplicate records across any layer.

## Decision
Implement a file-based existence check before processing each stage:
*   **Bronze & Silver Layers:** Verify the existence of the expected output file before execution.
*   **Gold Layer:** Utilize a sentinel/marker file pattern (e.g., `.done_YYYYMMDD`) to signify completion.
*   **Execution Logic:** If the target file or marker already exists, the task will skip processing and exit with a **success code (0)** to avoid failing the pipeline.

## Consequences
*   **Resiliency:** The pipeline can be safely re-run at any time without the risk of data duplication.
*   **Fault Tolerance:** Automatic retries in Airflow will not corrupt downstream data.
*   **Trade-off:** Forced reprocessing (backfilling) requires manual intervention to delete the existing files or markers.

## Alternatives Considered
*   **Truncate and Reload** — Simple to implement but lacks safety; if a crash occurs during the reload, the data state becomes inconsistent.
*   **Query-time Deduplication** — Computationally expensive at scale and fails to prevent "data bloat" in storage.
*   **Database UNIQUE Constraints** — Not applicable as the storage layer is built on **Apache Parquet**, which does not natively enforce relational constraints.