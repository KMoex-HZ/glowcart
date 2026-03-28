import pandas as pd
import os
from datetime import datetime
from utils.logger import get_logger

logger = get_logger("silver")

# Constants for schema enforcement
VALID_EVENT_TYPES = [
    'page_view', 'add_to_cart', 'checkout', 'payment_success', 'payment_failed'
]

# Path configurations
date_str = datetime.now().strftime('%Y%m%d')
bronze_path = f'/root/glowcart/storage/bronze/events/date={date_str}/events.parquet'
silver_dir = f'/root/glowcart/storage/silver/events/date={date_str}'
os.makedirs(silver_dir, exist_ok=True)

# --- Idempotency Check ---
# Prevent duplicate processing if Silver file already exists for today
silver_path = f'{silver_dir}/events.parquet'
if os.path.exists(silver_path):
    logger.info(f"Skipping: Silver layer for {date_str} already exists.")
    raise SystemExit(0)

logger.info("Reading Bronze layer (Raw Parquet)...")
df = pd.read_parquet(bronze_path)
logger.info(f"Initial record count: {len(df)}")

# --- Data Cleaning & Feature Engineering ---
logger.info("Starting cleaning process...")

# Remove exact duplicates based on unique event identifier
before = len(df)
df = df.drop_duplicates(subset=['event_id'])
logger.info(f"Deduplication: Removed {before - len(df)} rows")

# Standardize timestamps and extract temporal features
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['ingested_at'] = pd.to_datetime(df['ingested_at'])
df['hour'] = df['timestamp'].dt.hour
df['date'] = df['timestamp'].dt.date
df['quantity'] = df['quantity'].fillna(0).astype(int)
df['total_amount'] = df['total_amount'].fillna(0).astype(int)
df['is_transaction'] = df['event_type'].isin(
    ['add_to_cart', 'checkout', 'payment_success', 'payment_failed']
)

logger.info("Running Data Quality (DQ) assertions...")

checks = []

def run_check(name, description, passed, details=""):
    checks.append({
        "check": name,
        "description": description,
        "status": "PASS" if passed else "FAIL",
        "details": details
    })
    return passed

# Schema checks
for col in ['event_id', 'event_type', 'timestamp', 'total_amount']:
    run_check(
        f"column_exists:{col}",
        f"Column '{col}' must exist",
        col in df.columns,
        "" if col in df.columns else f"Column '{col}' is missing"
    )

# Nullability
run_check("null:event_id", "event_id must not be null",
          not df['event_id'].isnull().any(),
          f"{df['event_id'].isnull().sum()} null values found")

run_check("null:event_type", "event_type must not be null",
          not df['event_type'].isnull().any(),
          f"{df['event_type'].isnull().sum()} null values found")

# Uniqueness
run_check("duplicate:event_id", "event_id must be unique",
          not df['event_id'].duplicated().any(),
          f"{df['event_id'].duplicated().sum()} duplicates found")

# Value constraint
invalid_types = ~df['event_type'].isin(VALID_EVENT_TYPES)
run_check("invalid_value:event_type", f"event_type must be in {VALID_EVENT_TYPES}",
          not invalid_types.any(),
          f"{invalid_types.sum()} invalid values: {df.loc[invalid_types, 'event_type'].unique().tolist()}")

# Range checks
run_check("out_of_range:total_amount", "total_amount must be between 0 and 100,000,000",
          df['total_amount'].between(0, 100_000_000).all(),
          f"{(~df['total_amount'].between(0, 100_000_000)).sum()} out-of-range values")

run_check("out_of_range:quantity", "quantity must be between 0 and 1000",
          df['quantity'].between(0, 1000).all(),
          f"{(~df['quantity'].between(0, 1000)).sum()} out-of-range values")

# --- Generate HTML Report ---
def generate_dq_report(checks, df, date_str):
    report_dir = '/root/glowcart/docs/quality-reports'
    os.makedirs(report_dir, exist_ok=True)

    total = len(checks)
    passed = sum(1 for c in checks if c['status'] == 'PASS')
    failed = total - passed
    overall = "PASSED" if failed == 0 else "FAILED"
    overall_color = "#2e7d32" if failed == 0 else "#c62828"
    generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    rows = ""
    for c in checks:
        color = "#2e7d32" if c['status'] == 'PASS' else "#c62828"
        bg = "#f1f8e9" if c['status'] == 'PASS' else "#ffebee"
        rows += f"""
        <tr style="background:{bg}">
            <td>{c['check']}</td>
            <td>{c['description']}</td>
            <td style="color:{color}; font-weight:600">{c['status']}</td>
            <td>{c['details']}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>GlowCart DQ Report — {date_str}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; color: #212121; }}
        h1 {{ font-size: 22px; margin-bottom: 4px; }}
        .meta {{ color: #757575; font-size: 13px; margin-bottom: 24px; }}
        .summary {{ display: flex; gap: 16px; margin-bottom: 28px; }}
        .card {{ padding: 16px 24px; border-radius: 8px; min-width: 120px; }}
        .overall {{ background: {overall_color}; color: white; font-size: 18px; font-weight: 700; }}
        .pass {{ background: #f1f8e9; color: #2e7d32; font-size: 18px; font-weight: 700; }}
        .fail {{ background: #ffebee; color: #c62828; font-size: 18px; font-weight: 700; }}
        .label {{ font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
        th {{ background: #f5f5f5; padding: 10px 12px; text-align: left; border-bottom: 2px solid #e0e0e0; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #eeeeee; }}
        .footer {{ margin-top: 32px; font-size: 12px; color: #9e9e9e; }}
    </style>
</head>
<body>
    <h1>GlowCart — Data Quality Report</h1>
    <div class="meta">Layer: Silver &nbsp;|&nbsp; Date: {date_str} &nbsp;|&nbsp; Generated: {generated_at} &nbsp;|&nbsp; Records: {len(df):,}</div>
    <div class="summary">
        <div class="card overall"><div class="label">Overall</div>{overall}</div>
        <div class="card pass"><div class="label">Passed</div>{passed}/{total}</div>
        <div class="card fail"><div class="label">Failed</div>{failed}/{total}</div>
    </div>
    <table>
        <thead>
            <tr>
                <th>Check</th>
                <th>Description</th>
                <th>Status</th>
                <th>Details</th>
            </tr>
        </thead>
        <tbody>{rows}</tbody>
    </table>
    <div class="footer">GlowCart Data Engineering Portfolio &nbsp;·&nbsp; Khairunnisa Maharani</div>
</body>
</html>"""

    report_path = f"{report_dir}/dq_report_{date_str}.html"
    with open(report_path, 'w') as f:
        f.write(html)
    logger.info(f"DQ report saved: {report_path}")

generate_dq_report(checks, df, date_str)

# --- Validation Evaluation ---
errors = [c['check'] for c in checks if c['status'] == 'FAIL']
if errors:
    logger.error("DQ VALIDATION FAILED. Critical issues detected:")
    for err in errors:
        logger.error(f"  - {err}")
    logger.error("Halting pipeline: Data will not be written to Silver layer.")
    raise SystemExit(1)

logger.info("DQ Validation passed. Data is verified clean.")

df.to_parquet(silver_path, index=False)
logger.info(f"Successfully saved to Silver layer: {len(df)} records")
logger.info(f"Target path: {silver_path}")