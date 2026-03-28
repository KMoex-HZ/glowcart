from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import os
from utils.logger import get_logger

logger = get_logger("spark")

# Set Python environment for Spark executors
os.environ['PYSPARK_PYTHON'] = '/root/glowcart/.venv/bin/python3'

# Initialize Spark Session with performance optimizations
spark = SparkSession.builder \
    .appName("GlowCart-Transform") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.ui.enabled", "false") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

# Suppress verbose logs for cleaner output
spark.sparkContext.setLogLevel("ERROR")

logger.info("GlowCart PySpark Transformation Engine")
logger.info("-" * 40)

# Environment & Path setup
date_str = datetime.now().strftime('%Y%m%d')
silver_path = f'/root/glowcart/storage/silver/events/date={date_str}/events.parquet'

logger.info(f"Loading Silver layer data for: {date_str}")
df = spark.read.parquet(silver_path)
logger.info(f"Record Count: {df.count()}")

# Analysis 1: Geographic Performance (City Segmentation)
# Purpose: Identifying top revenue-generating regions
city_stats = df.groupBy('user_city') \
    .agg(
        F.count('event_id').alias('total_events'),
        F.countDistinct('user_id').alias('unique_users'),
        F.sum(
            F.when(F.col('event_type') == 'payment_success', F.col('total_amount'))
            .otherwise(0)
        ).alias('total_revenue')
    ) \
    .orderBy(F.col('total_revenue').desc()) \
    .limit(5)

logger.info("Top 5 Cities by Revenue Calculated.")
city_stats.show(truncate=False)

# Analysis 2: Product Conversion & Performance
# Purpose: Tracking funnel conversion from views to purchases per product
product_perf = df.groupBy('product_name', 'product_category') \
    .agg(
        F.count(F.when(F.col('event_type') == 'page_view', 1)).alias('views'),
        F.count(F.when(F.col('event_type') == 'add_to_cart', 1)).alias('add_to_carts'),
        F.count(F.when(F.col('event_type') == 'payment_success', 1)).alias('purchases'),
        F.sum(
            F.when(F.col('event_type') == 'payment_success', F.col('total_amount'))
            .otherwise(0)
        ).alias('revenue')
    ) \
    .withColumn('cart_conversion_pct',
        F.round(
            F.when(F.col('views') > 0, F.col('add_to_carts') / F.col('views') * 100)
            .otherwise(0), 1
        )
    ) \
    .orderBy(F.col('revenue').desc())

logger.info("Product Performance & Conversion Metrics Ready.")
product_perf.show(truncate=False)

# Analysis 3: Infrastructure breakdown (Device & Platform)
# Purpose: Understanding user tech-stack distribution
device_analysis = df.groupBy('device', 'platform') \
    .agg(
        F.count('event_id').alias('total_events'),
        F.sum(
            F.when(F.col('event_type') == 'payment_success', F.col('total_amount'))
            .otherwise(0)
        ).alias('revenue')
    ) \
    .orderBy(F.col('total_events').desc())

logger.info("Tech-stack Distribution Analysis Completed.")
device_analysis.show(truncate=False)

# Materialization to Gold Layer (Spark Output)
output_base = '/root/glowcart/storage/gold/spark'
os.makedirs(output_base, exist_ok=True)

# Persisting small aggregated results as Parquet for API consumption
city_stats.toPandas().to_parquet(f'{output_base}/city_stats.parquet', index=False)
product_perf.toPandas().to_parquet(f'{output_base}/product_performance.parquet', index=False)
device_analysis.toPandas().to_parquet(f'{output_base}/device_analysis.parquet', index=False)

logger.info("-" * 40)
logger.info(f"Spark Job Successful. Gold tables persisted in: {output_base}")
spark.stop()