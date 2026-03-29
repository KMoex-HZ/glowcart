# Runbook: Data Quality Failure

**Document:** DATA_QUALITY_FAILURE.md  
**Scope:** Great Expectations validation at the Silver layer  
**Last Updated:** 2026-03-28

---

## When to Use This Runbook

Use this runbook when:
- The Silver layer pipeline exits with `GE validation failed` in the logs
- An Airflow task named `validate_silver` or `bronze_to_silver` shows `failed`
- A Great Expectations HTML report in `docs/quality-reports/` shows failed expectations
- Downstream Gold layer data looks suspicious (unexpected nulls, revenue spikes, missing event types)

---

## Understanding the Gating Mechanism

GlowCart's Silver layer uses **Great Expectations as a hard gate**: if any expectation fails, the pipeline stops immediately with a non-zero exit code. Data does not proceed to Gold.

This is intentional. Letting bad data reach the Gold layer — and subsequently the dashboard — is worse than halting the pipeline. Stale-but-correct data is preferable to fresh-but-wrong data.

---

## Step 1 — Read the Validation Report

Great Expectations generates an HTML report for every validation run. Open the latest report:

```bash
# List reports, newest first
ls -lt docs/quality-reports/

# Open in browser (WSL2)
explorer.exe docs/quality-reports/<latest_report>.html
```

The report shows:
- Which expectations passed (green) and failed (red)
- The observed value vs. the expected value for each check
- The percentage of failing rows for column-level checks

Note down which expectations failed before proceeding.

---

## Step 2 — Identify the Root Cause

Cross-reference the failed expectations with the likely sources:

| Failed Expectation | Likely Cause |
|---|---|
| `expect_column_values_to_not_be_null` on `event_id` | Producer bug — events generated without IDs |
| `expect_column_values_to_be_in_set` on `event_type` | New event type added to producer but not allowlist |
| `expect_column_values_to_be_between` on `amount` | Negative amounts from refund events; or data corruption |
| `expect_column_to_exist` | Schema change — column renamed or removed at Bronze |
| High null rate on `user_id` | Unauthenticated session events leaking into pipeline |

Check the Bronze layer raw data to confirm:

```bash
python3 -c "
import pandas as pd
df = pd.read_parquet('storage/bronze/')
print(df.dtypes)
print(df.isnull().sum())
print(df['event_type'].value_counts())
"
```

---

## Step 3 — Determine Response

### Option A — Data issue is upstream (producer / generator)

If the bad data came from the event generator or a schema change:

1. Fix the producer script (`ingestion/scripts/bulk_generate.py` or `ingestion/kafka/producer.py`).
2. Re-generate clean events for the affected date.
3. Re-ingest to Bronze (idempotent — safe to rerun).
4. Rerun Silver validation.

### Option B — Expectation is too strict

If the data is actually valid but the expectation was wrong (e.g., a new legitimate event type was added):

1. Update the Expectation Suite in `storage/great_expectations/expectations/`.
2. Document the change in a comment — why the expectation was relaxed or updated.
3. If this is a breaking schema change, create a new ADR documenting the decision.
4. Rerun Silver validation.

### Option C — Known anomaly, proceed with filtering

If the failing rows are a known edge case that should be excluded (not fixed):

1. Add a pre-validation filter in `storage/silver/bronze_to_silver.py` to drop the offending rows.
2. Route the dropped rows to the DLQ topic (`events.dlq`) for audit — do not silently discard.
3. Rerun Silver validation.

---

## Step 4 — Rerun Silver Validation

After addressing the root cause:

```bash
source .venv/bin/activate
python3 storage/silver/bronze_to_silver.py
```

A new HTML report will be generated in `docs/quality-reports/`. Confirm all expectations pass (100% green) before continuing.

---

## Step 5 — Continue the Pipeline

Once Silver validation passes, run Gold:

```bash
python3 storage/gold/silver_to_gold.py
cd transform/dbt && dbt run
```

Verify the dashboard reflects updated data at `http://localhost:8000`.

---

## Step 6 — Post-Incident Review

For any validation failure that caused a pipeline halt, record the following in a comment or a brief note in `docs/quality-reports/`:

- Date and time of failure
- Which expectation(s) failed
- Root cause (producer bug / schema change / expectation misconfiguration)
- Resolution taken (Option A / B / C above)
- Whether an ADR update is needed

This creates an audit trail of data quality decisions — important for fintech and regulated domains.

---

## Relevant Files

| File | Purpose |
|---|---|
| `storage/great_expectations/expectations/` | Expectation Suite definitions |
| `storage/silver/bronze_to_silver.py` | Validation entry point |
| `docs/quality-reports/` | HTML validation reports |
| `docs/adr/ADR-003-data-quality-validation.md` | Decision record for this gating approach |
