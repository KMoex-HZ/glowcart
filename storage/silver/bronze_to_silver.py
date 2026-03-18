import pandas as pd
import os
from datetime import datetime

date_str = datetime.now().strftime('%Y%m%d')
bronze_path = f'/root/glowcart/storage/bronze/events/date={date_str}/events.parquet'
silver_dir = f'/root/glowcart/storage/silver/events/date={date_str}'
os.makedirs(silver_dir, exist_ok=True)

print("Reading Bronze layer...")
df = pd.read_parquet(bronze_path)
print(f"  Raw records: {len(df)}")

print("\nCleaning data...")

before = len(df)
df = df.drop_duplicates(subset=['event_id'])
print(f"  Duplicates removed: {before - len(df)} rows")

df['timestamp'] = pd.to_datetime(df['timestamp'])
df['ingested_at'] = pd.to_datetime(df['ingested_at'])
df['hour'] = df['timestamp'].dt.hour
df['date'] = df['timestamp'].dt.date
print(f"  Timestamps converted to datetime ✅")

df['quantity'] = df['quantity'].fillna(0).astype(int)
df['total_amount'] = df['total_amount'].fillna(0).astype(int)
print(f"  Null values filled ✅")

df['is_transaction'] = df['event_type'].isin(
    ['add_to_cart', 'checkout', 'payment_success', 'payment_failed']
)
print(f"  is_transaction column added ✅")

silver_path = f'{silver_dir}/events.parquet'
df.to_parquet(silver_path, index=False)

print(f"\n✅ Silver layer saved: {len(df)} records")
print(f"   Path: {silver_path}")
print(f"\nSilver schema:")
print(df.dtypes)
print(f"\nEvent type distribution:")
print(df['event_type'].value_counts())
