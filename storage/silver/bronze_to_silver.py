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

# Handle missing values and cast types for downstream stability
df['quantity'] = df['quantity'].fillna(0).astype(int)
df['total_amount'] = df['total_amount'].fillna(0).astype(int)

# Create boolean flag for transactional events
df['is_transaction'] = df['event_type'].isin(
    ['add_to_cart', 'checkout', 'payment_success', 'payment_failed']
)

# --- Data Quality Gating (Pandas-based) ---
# Manual assertions to ensure data integrity before persistence
logger.info("Running Data Quality (DQ) assertions...")
errors = []

# Schema Check
for col in ['event_id', 'event_type', 'timestamp', 'total_amount']:
    if col not in df.columns:
        errors.append(f"missing_column:{col}")

# Nullability Check
if df['event_id'].isnull().any():
    errors.append("null:event_id")
if df['event_type'].isnull().any():
    errors.append("null:event_type")

# Uniqueness Check
if df['event_id'].duplicated().any():
    errors.append("duplicate:event_id")

# Value Constraint Check
if not df['event_type'].isin(VALID_EVENT_TYPES).all():
    errors.append("invalid_value:event_type")

# Range/Boundary Checks
if not df['total_amount'].between(0, 100_000_000).all():
    errors.append("out_of_range:total_amount")
if not df['quantity'].between(0, 1000).all():
    errors.append("out_of_range:quantity")

# --- Validation Evaluation ---
if errors:
    logger.error("DQ VALIDATION FAILED. Critical issues detected:")
    for err in errors:
        logger.error(f"  - {err}")
    logger.error("Halting pipeline: Data will not be written to Silver layer.")
    raise SystemExit(1)

logger.info("DQ Validation passed. Data is verified clean.")

# --- Persistence: Write to Silver Layer ---
df.to_parquet(silver_path, index=False)

logger.info(f"Successfully saved to Silver layer: {len(df)} records")
logger.info(f"Target path: {silver_path}")