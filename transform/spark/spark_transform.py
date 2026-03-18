from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import os

os.environ['PYSPARK_PYTHON'] = '/root/glowcart/.venv/bin/python3'

spark = SparkSession.builder \
    .appName("GlowCart-Transform") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.ui.enabled", "false") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 50)
print("GlowCart PySpark Transform Job")
print("=" * 50)

date_str = datetime.now().strftime('%Y%m%d')
silver_path = f'/root/glowcart/storage/silver/events/date={date_str}/events.parquet'

print(f"\nReading Silver layer...")
df = spark.read.parquet(silver_path)
print(f"Total records: {df.count()}")

# --- Transformasi 1: Customer segmentation by city ---
print("\n📊 Customer Segmentation by City:")
city_stats = df.groupBy('user_city') \
    .agg(
        F.count('event_id').alias('total_events'),
        F.countDistinct('user_id').alias('unique_users'),
        F.sum(F.when(F.col('event_type') == 'payment_success',
            F.col('total_amount')).otherwise(0)).alias('total_revenue')
    ) \
    .orderBy(F.col('total_revenue').desc()) \
    .limit(5)
city_stats.show(truncate=False)

# --- Transformasi 2: Product performance ---
print("📊 Product Performance:")
product_perf = df.groupBy('product_name', 'product_category') \
    .agg(
        F.count(F.when(F.col('event_type') == 'page_view', 1)).alias('views'),
        F.count(F.when(F.col('event_type') == 'add_to_cart', 1)).alias('add_to_carts'),
        F.count(F.when(F.col('event_type') == 'payment_success', 1)).alias('purchases'),
        F.sum(F.when(F.col('event_type') == 'payment_success',
            F.col('total_amount')).otherwise(0)).alias('revenue')
    ) \
    .withColumn('cart_conversion_pct',
        F.round(
            F.when(F.col('views') > 0,
                F.col('add_to_carts') / F.col('views') * 100
            ).otherwise(0),
        1)
    ) \
    .orderBy(F.col('revenue').desc())
product_perf.show(truncate=False)

# --- Transformasi 3: Device & platform analysis ---
print("📊 Device & Platform Analysis:")
device_analysis = df.groupBy('device', 'platform') \
    .agg(
        F.count('event_id').alias('total_events'),
        F.sum(F.when(F.col('event_type') == 'payment_success',
            F.col('total_amount')).otherwise(0)).alias('revenue')
    ) \
    .orderBy(F.col('total_events').desc())
device_analysis.show(truncate=False)

# --- Simpan hasil ke Gold layer ---
output_base = '/root/glowcart/storage/gold/spark'
os.makedirs(output_base, exist_ok=True)

city_stats.toPandas().to_parquet(f'{output_base}/city_stats.parquet', index=False)
product_perf.toPandas().to_parquet(f'{output_base}/product_performance.parquet', index=False)
device_analysis.toPandas().to_parquet(f'{output_base}/device_analysis.parquet', index=False)

print("\n✅ Spark transform done — results saved to Gold/spark/")
spark.stop()
