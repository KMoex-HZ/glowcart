from kafka import KafkaProducer
import json
import time
import sys
from datetime import datetime

# --- Environment Setup ---
sys.path.append('/root/glowcart')
from ingestion.scripts.generate_events import generate_user, generate_event

# Configuration
TOPIC = 'glowcart-events'
BOOTSTRAP_SERVERS = 'localhost:9092'

def run_producer():
    """
    Main loop to generate and send synthetic e-commerce events to Kafka.
    """
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )

    print(f"GlowCart Producer started — streaming to topic: {TOPIC}")
    print("Press Ctrl+C to exit safely\n")

    count = 0
    try:
        while True:
            user = generate_user()
            event = generate_event(user)

            # Send event to Kafka
            producer.send(TOPIC, value=event)
            count += 1

            # Console logging for monitoring
            print(
                f"[{count}] {datetime.now().strftime('%H:%M:%S')} | "
                f"Event: {event['event_type']} | "
                f"User: {event['user']['name']} | "
                f"City: {event['user']['city']}"
            )

            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping producer... Closing connections.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run_producer()