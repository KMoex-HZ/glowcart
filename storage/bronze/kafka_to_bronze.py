from kafka import KafkaConsumer
import json
import pandas as pd
import os
from datetime import datetime
from utils.logger import get_logger

logger = get_logger("bronze")

# --- Configurations ---
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'glowcart-events'

date_str = datetime.now().strftime('%Y%m%d')
output_path = f'/root/glowcart/storage/bronze/events/date={date_str}/events.parquet'

# Ensure idempotency: skip processing if the file for today already exists
if os.path.exists(output_path):
    logger.info(f"Skipping: Bronze layer for {date_str} already exists at {output_path}")
    raise SystemExit(0)

os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Initialize consumer with 5s timeout to stop once the batch is consumed
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

logger.info("Consuming events from Kafka...")

events = []
for message in consumer:
    event = message.value
    # Manual flattening for schema control
    flat_event = {
        'event_id':       event['event_id'],
        'event_type':     event['event_type'],
        'timestamp':      event['timestamp'],
        'session_id':     event['session_id'],
        'device':         event['device'],
        'platform':       event['platform'],
        'user_id':        event['user']['user_id'],
        'user_name':      event['user']['name'],
        'user_email':     event['user']['email'],
        'user_city':      event['user']['city'],
        'user_age':       event['user']['age'],
        'product_id':     event['product']['id'],
        'product_name':   event['product']['name'],
        'product_price':  event['product']['price'],
        'product_category': event['product']['category'],
        'quantity':       event.get('quantity'),
        'total_amount':   event.get('total_amount'),
        'ingested_at':    datetime.now().isoformat(),
    }
    events.append(flat_event)

consumer.close()

if events:
    df = pd.DataFrame(events)
    df.to_parquet(output_path, index=False)
    logger.info(f"Successfully ingested {len(df)} events to {output_path}")
else:
    logger.warning("No new events found in Kafka topic.")