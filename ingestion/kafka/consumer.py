from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, timezone

# --- Configurations ---
MAIN_TOPIC = 'glowcart-events'
DLQ_TOPIC = 'glowcart-events.dlq'
BOOTSTRAP_SERVERS = 'localhost:9092'

# Essential fields for downstream processing
REQUIRED_FIELDS = ['event_type', 'user', 'product', 'total_amount']

def validate_event(event: dict) -> list[str]:
    """
    Validates the event structure. 
    Returns a list of error codes; an empty list indicates a valid event.
    """
    errors = []
    for field in REQUIRED_FIELDS:
        if field not in event:
            errors.append(f"missing_field:{field}")
            
    # Nested validation for user and product objects
    if 'user' in event and 'name' not in event.get('user', {}):
        errors.append("missing_field:user.name")
    if 'product' in event and 'name' not in event.get('product', {}):
        errors.append("missing_field:product.name")
        
    return errors

def send_to_dlq(original_event, errors: list[str], offset: int, producer: KafkaProducer):
    """
    Redirects invalid events to the DLQ topic with error metadata.
    """
    dlq_payload = {
        "original_event": original_event,
        "errors": errors,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "source_topic": MAIN_TOPIC,
        "source_offset": offset
    }
    producer.send(DLQ_TOPIC, value=dlq_payload)
    producer.flush()
    print(f"[DLQ] Offset {offset} -> {errors}")

def process_event(event: dict, offset: int):
    """
    Placeholder for successful event processing.
    """
    amount_str = f"Rp {event['total_amount']:,}" if event.get('total_amount') else "no amount"
    print(
        f"[OK] [{offset}] "
        f"{event['event_type']} | "
        f"{event['user']['name']} | "
        f"{event['product']['name']} | "
        f"{amount_str}"
    )

if __name__ == "__main__":
    consumer = KafkaConsumer(
        MAIN_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    dlq_producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("GlowCart Consumer started — listening for events...")
    print(f"Main Topic: {MAIN_TOPIC} | DLQ Topic: {DLQ_TOPIC}\n")

    try:
        for message in consumer:
            event = message.value
            errors = validate_event(event)

            if errors:
                # Send to DLQ if validation fails
                send_to_dlq(event, errors, message.offset, dlq_producer)
            else:
                # Business logic for valid events
                process_event(event, message.offset)
    except KeyboardInterrupt:
        print("\nConsumer stopped manually.")
    finally:
        consumer.close()
        dlq_producer.close()