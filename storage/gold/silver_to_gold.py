import pandas as pd
import os
from datetime import datetime

date_str = datetime.now().strftime('%Y%m%d')
silver_path = f'/root/glowcart/storage/silver/events/date={date_str}/events.parquet'
gold_dir = '/root/glowcart/storage/gold'
os.makedirs(gold_dir, exist_ok=True)

print("Reading Silver layer...")
df = pd.read_parquet(silver_path)

# --- Table 1: Revenue by product category ---
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
print("\nRevenue by Category:")
print(revenue_by_category.to_string(index=False))

# --- Table 2: Conversion funnel ---
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
print("\nConversion Funnel:")
print(funnel.to_string(index=False))

# --- Table 3: Top products by revenue ---
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
print("\nTop Products:")
print(top_products.to_string(index=False))

# --- Table 4: Hourly activity ---
hourly_activity = (
    df.groupby('hour')
    .agg(total_events=('event_id', 'count'))
    .reset_index()
    .sort_values('hour')
)
hourly_activity.to_parquet(f'{gold_dir}/hourly_activity.parquet', index=False)
print("\nHourly Activity:")
print(hourly_activity.to_string(index=False))

print(f"\nGold layer complete — 4 business tables ready!")
