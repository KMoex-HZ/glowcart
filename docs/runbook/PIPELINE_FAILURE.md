# Runbook: Pipeline Failure

**Document:** PIPELINE_FAILURE.md  
**Scope:** GlowCart data pipeline — Bronze ingestion, Silver transformation, Gold aggregation  
**Last Updated:** 2025-04-01

---

## When to Use This Runbook

Use this runbook when:
- An Airflow task shows status `failed` or `up_for_retry`
- The pipeline exits with a non-zero exit code when run manually
- The FastAPI dashboard is showing stale data (no updates in the expected window)
- A Slack/email alert fires for pipeline SLA miss

---

## Step 1 — Identify Which Stage Failed

Open the Airflow UI at `http://localhost:8080`. Navigate to the DAG and click the failed task (shown in red).

Check the task log. The last few lines will tell you which script failed and why. Common indicators:

| Log message | Likely cause |
|---|---|
| `Connection refused` on port 9092 | Kafka is not running |
| `FileNotFoundError` on a Parquet path | Upstream stage did not complete |
| `GE validation failed` | Data quality issue — see [DATA_QUALITY_FAILURE.md](DATA_QUALITY_FAILURE.md) |
| `Java gateway process exited` | PySpark / Java issue |
| `No module named ...` | Python environment issue |

---

## Step 2 — Check Infrastructure Health

Before rerunning anything, verify the stack is up:

```bash
docker compose ps
```

All services should show `Up`. If any are `Exited`:

```bash
# Restart a specific service
docker compose restart kafka

# Or restart everything
docker compose up -d
```

Wait 30 seconds after restarting Kafka before rerunning any pipeline step — brokers need time to elect a leader.

---

## Step 3 — Rerun the Failed Stage

Each pipeline stage is **idempotent** — safe to rerun without producing duplicate data. The stage will detect existing output and either skip or overwrite cleanly.

```bash
# Activate environment
source .venv/bin/activate

# Rerun only the failed stage
python3 storage/bronze/kafka_to_bronze.py   # if Bronze failed
python3 storage/silver/bronze_to_silver.py  # if Silver failed
python3 storage/gold/silver_to_gold.py      # if Gold failed
```

If the stage succeeds manually but fails in Airflow, clear the task in the Airflow UI and let it retry:

1. Click the failed task in the DAG graph view
2. Click **Clear** → confirm
3. Airflow will requeue the task on the next scheduler tick

---

## Step 4 — Reprocess Data from a Specific Date

If you need to backfill or reprocess a specific date partition:

```bash
# Set the target date
export PROCESSING_DATE=2025-03-15

# Run with date override (pipeline reads PROCESSING_DATE env var)
PROCESSING_DATE=2025-03-15 python3 storage/bronze/kafka_to_bronze.py
PROCESSING_DATE=2025-03-15 python3 storage/silver/bronze_to_silver.py
PROCESSING_DATE=2025-03-15 python3 storage/gold/silver_to_gold.py
```

The idempotency checkpoint will detect the existing batch and skip it. To force reprocessing, delete the checkpoint entry first:

```bash
# Remove the checkpoint for that date (adjust path as needed)
rm -f storage/.checkpoints/bronze_2025-03-15.checkpoint
rm -f storage/.checkpoints/silver_2025-03-15.checkpoint
rm -f storage/.checkpoints/gold_2025-03-15.checkpoint
```

Then rerun the stages above.

---

## Step 5 — Inspect the Dead Letter Queue

If Bronze ingestion failed due to bad events, check how many events landed in the DLQ:

```bash
python3 ingestion/scripts/dlq_inspector.py
```

This will print a summary of DLQ events grouped by `error_reason`. If the volume is unexpectedly high (> 1% of total events), investigate the producer — the event schema may have changed.

To replay DLQ events after fixing the root cause:

```bash
python3 ingestion/scripts/dlq_replay.py --date 2025-03-15
```

---

## Step 6 — Verify Recovery

After rerunning, confirm the pipeline completed successfully:

```bash
# Check Gold layer output exists and has recent data
ls -lh storage/gold/

# Hit the API health endpoint
curl http://localhost:8000/health

# Spot-check revenue endpoint
curl http://localhost:8000/api/revenue
```

If the dashboard still shows stale data, hard-refresh the browser (Ctrl+Shift+R) — Chart.js may be serving a cached response.

---

## Escalation

If the pipeline is still failing after following these steps:

1. Check `docs/adr/` for the relevant architectural decision — the ADR often explains constraints that affect recovery options.
2. Check `tests/` — run `pytest tests/ -v` to confirm the core transformation logic is still valid.
3. If a library version conflict is suspected, recreate the virtual environment: `rm -rf .venv && python3 -m venv .venv && pip install -r requirements.txt`.