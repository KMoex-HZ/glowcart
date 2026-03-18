from kafka import KafkaConsumer
import json
import pandas as pd
import os
from datetime import datetime

os.makedirs('/root/glowcart/storage/bronze/events', exist_ok=True)

consumer = KafkaConsumer(
    'glowcart-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Membaca events dari Kafka ke Bronze layer...")

events = []
for message in consumer:
    event = message.value
    flat_event = {
        'event_id': event['event_id'],
        'event_type': event['event_type'],
        'timestamp': event['timestamp'],
        'session_id': event['session_id'],
        'device': event['device'],
        'platform': event['platform'],
        'user_id': event['user']['user_id'],
        'user_name': event['user']['name'],
        'user_email': event['user']['email'],
        'user_city': event['user']['city'],
        'user_age': event['user']['age'],
        'product_id': event['product']['id'],
        'product_name': event['product']['name'],
        'product_price': event['product']['price'],
        'product_category': event['product']['category'],
        'quantity': event.get('quantity'),
        'total_amount': event.get('total_amount'),
        'ingested_at': datetime.now().isoformat(),
    }
    events.append(flat_event)
    print(f"  Read: {event['event_type']} | {event['user']['name']}")

consumer.close()

if events:
    df = pd.DataFrame(events)
    date_str = datetime.now().strftime('%Y%m%d')
    output_path = f'/root/glowcart/storage/bronze/events/date={date_str}/events.parquet'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"\n✅ Saved {len(df)} events ke Bronze layer")
    print(f"   Path: {output_path}")
    print(f"\nSchema DataFrame:")
    print(df.dtypes)
    print(f"\nSample data:")
    print(df[['event_type','user_name','product_name','total_amount']].head(5))
else:
    print("Tidak ada events ditemukan di Kafka.")
