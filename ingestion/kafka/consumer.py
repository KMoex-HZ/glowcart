from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'glowcart-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("GlowCart Consumer started — membaca events dari Kafka...")
print("Tekan Ctrl+C untuk stop\n")

for message in consumer:
    event = message.value
    print(f"Received [{message.offset}]: "
          f"{event['event_type']} | "
          f"{event['user']['name']} | "
          f"{event['product']['name']} | "
          f"Rp {event['total_amount']:,}" if event['total_amount'] else 
          f"Received [{message.offset}]: "
          f"{event['event_type']} | "
          f"{event['user']['name']} | "
          f"{event['product']['name']}")
