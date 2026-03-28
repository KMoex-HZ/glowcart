# ADR-004: Dead Letter Queue for Error Handling in Kafka Consumer

**Status:** Accepted  
**Date:** 2026-03-29  
**Author:** Khairunnisa Maharani

## Context
The Kafka consumer will inevitably encounter corrupt events, invalid schemas, or business rule violations. The ingestion pipeline must remain resilient and should not crash or stall due to a single "poison pill" record.

## Decision
Implement a dedicated Kafka topic named `glowcart-events.dlq` to serve as a **Dead Letter Queue (DLQ)**. Any event that fails validation will be redirected to this topic, enriched with the **error reason**, **original payload**, and **processing timestamp**.

## Consequences
*   **Pipeline Resiliency:** The consumer continues processing subsequent records without interruption.
*   **Observability & Replayability:** Erroneous events can be analyzed for debugging or re-driven into the pipeline once the underlying issue is resolved.
*   **Trade-off:** Introduces operational overhead as the DLQ topic requires separate monitoring and a defined strategy for message purging or reprocessing.

## Alternatives Considered
*   **Skip bad records** — Data is lost without a trace, making audits and data recovery impossible.
*   **Crash on error** — Halts the entire pipeline, requiring manual intervention and increasing downtime.
*   **Infinite Retry** — Risk of entering an infinite loop for inherently corrupt data that can never be successfully processed.