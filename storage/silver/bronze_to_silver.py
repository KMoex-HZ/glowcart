import pandas as pd
import os
from datetime import datetime

date_str = datetime.now().strftime('%Y%m%d')
bronze_path = f'/root/glowcart/storage/bronze/events/date={date_str}/events.parquet'
silver_dir = f'/root/glowcart/storage/silver/events/date={date_str}'
os.makedirs(silver_dir, exist_ok=True)

print("Membaca Bronze layer...")
df = pd.read_parquet(bronze_path)
print(f"  Raw records: {len(df)}")

print("\nCleaning data...")

before = len(df)
df = df.drop_duplicates(subset=['event_id'])
print(f"  Duplikat dihapus: {before - len(df)} rows")

df['timestamp'] = pd.to_datetime(df['timestamp'])
df['ingested_at'] = pd.to_datetime(df['ingested_at'])
df['hour'] = df['timestamp'].dt.hour
df['date'] = df['timestamp'].dt.date
print(f"  Timestamp dikonversi ke datetime ✅")

df['quantity'] = df['quantity'].fillna(0).astype(int)
df['total_amount'] = df['total_amount'].fillna(0).astype(int)
print(f"  NaN di quantity dan total_amount diisi 0 ✅")

df['is_transaction'] = df['event_type'].isin(
    ['add_to_cart', 'checkout', 'payment_success', 'payment_failed']
)
print(f"  Kolom is_transaction ditambahkan ✅")

silver_path = f'{silver_dir}/events.parquet'
df.to_parquet(silver_path, index=False)

print(f"\n✅ Silver layer saved: {len(df)} records")
print(f"   Path: {silver_path}")
print(f"\nSchema Silver:")
print(df.dtypes)
print(f"\nDistribusi event_type:")
print(df['event_type'].value_counts())
