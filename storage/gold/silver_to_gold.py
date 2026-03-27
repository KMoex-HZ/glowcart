import pandas as pd
import os
from datetime import datetime
from utils.logger import get_logger

logger = get_logger("gold")

# --- Environment Setup ---
date_str = datetime.now().strftime('%Y%m%d')
silver_path = f'/root/glowcart/storage/silver/events/date={date_str}/events.parquet'
gold_dir = '/root/glowcart/storage/gold'
os.makedirs(gold_dir, exist_ok=True)

# Idempotency: Skip if the gold layer for today is already processed
gold_marker = f'{gold_dir}/.done_{date_str}'
if os.path.exists(gold_marker):
    logger.info(f"Skipping: Gold build for {date_str} already completed.")
    raise SystemExit(0)

logger.info("Starting Gold layer transformation from Silver...")
df = pd.read_parquet(silver_path)

# Table 1: Revenue by Category (High-level business performance)
revenue_by_category = (
    df[df['event_type'] == 'payment_success']
    .groupby('product_category')
    .agg(
        total_revenue=('total_amount', 'sum'),
        total_orders=('event_id', 'count'),
        avg_order_value=('total_amount', 'mean')
    )
    .reset_index()
    .sort_values('total_revenue', ascending=False)
)
revenue_by_category.to_parquet(f'{gold_dir}/revenue_by_category.parquet', index=False)
logger.info(f"Revenue by Category generated: {len(revenue_by_category)} rows")

# Table 2: Conversion Funnel (Measuring user journey efficiency)
funnel = pd.DataFrame({
    'stage': ['page_view', 'add_to_cart', 'checkout', 'payment_success'],
    'count': [
        len(df[df['event_type'] == 'page_view']),
        len(df[df['event_type'] == 'add_to_cart']),
        len(df[df['event_type'] == 'checkout']),
        len(df[df['event_type'] == 'payment_success']),
    ]
})
funnel['conversion_rate_pct'] = (
    funnel['count'] / funnel['count'].iloc[0] * 100
).round(1)
funnel.to_parquet(f'{gold_dir}/conversion_funnel.parquet', index=False)
logger.info("Conversion Funnel metrics calculated")

# Table 3: Top Products (Product-level revenue tracking)
top_products = (
    df[df['event_type'] == 'payment_success']
    .groupby(['product_id', 'product_name', 'product_category'])
    .agg(
        total_revenue=('total_amount', 'sum'),
        units_sold=('quantity', 'sum')
    )
    .reset_index()
    .sort_values('total_revenue', ascending=False)
    .head(5)
)
top_products.to_parquet(f'{gold_dir}/top_products.parquet', index=False)
logger.info("Top 5 Products by Revenue updated")

# Table 4: Hourly Activity (Traffic trend analysis)
hourly_activity = (
    df.groupby('hour')
    .agg(total_events=('event_id', 'count'))
    .reset_index()
    .sort_values('hour')
)
hourly_activity.to_parquet(f'{gold_dir}/hourly_activity.parquet', index=False)
logger.info("Hourly activity trends updated")

# Success marker for Airflow/Orchestration tracking
open(gold_marker, 'w').close()
logger.info(f"Gold layer processing complete. Marker written: {gold_marker}")