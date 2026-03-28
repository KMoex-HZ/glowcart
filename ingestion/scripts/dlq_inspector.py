from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime

DLQ_TOPIC = 'glowcart-events.dlq'
BOOTSTRAP_SERVERS = 'localhost:9092'

print(f"{'='*60}")
print(f"  GlowCart — DLQ Inspector")
print(f"  Topic: {DLQ_TOPIC}")
print(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*60}\n")

consumer = KafkaConsumer(
    DLQ_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

events = []
error_counts = defaultdict(int)

for message in consumer:
    event = message.value
    events.append(event)
    for err in event.get('errors', []):
        error_counts[err] += 1

consumer.close()

if not events:
    print("No events in DLQ.")
else:
    print(f"Total DLQ events: {len(events)}\n")
    print("Error breakdown:")
    for err, count in sorted(error_counts.items(), key=lambda x: -x[1]):
        print(f"  {err:<40} {count} events")
    print(f"\nLast 5 events:")
    for e in events[-5:]:
        print(f"  [{e.get('failed_at', 'N/A')}] errors={e.get('errors')} offset={e.get('source_offset')}")

print(f"\n{'='*60}")
