from kafka import KafkaProducer
import json
import time
import sys
sys.path.append('/root/glowcart')
from ingestion.scripts.generate_events import generate_user, generate_event

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

print("GlowCart Producer started — mengirim events ke Kafka...")
print("Tekan Ctrl+C untuk stop\n")

count = 0
while True:
    user = generate_user()
    event = generate_event(user)
    
    producer.send('glowcart-events', value=event)
    count += 1
    
    print(f"[{count}] Sent: {event['event_type']} | "
          f"User: {event['user']['name']} | "
          f"Product: {event['product']['name']} | "
          f"City: {event['user']['city']}")
    
    time.sleep(1)
