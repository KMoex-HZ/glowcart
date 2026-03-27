from kafka import KafkaProducer
from faker import Faker
import json
import random
import sys
from datetime import datetime, timedelta

# --- Constants & Mock Data ---
fake = Faker('id_ID')
TOPIC = 'glowcart-events'
BOOTSTRAP_SERVERS = 'localhost:9092'
TARGET_EVENTS = 10000

PRODUCTS = [
    {"id": "P001", "name": "Sepatu Lari Nike", "price": 850000, "category": "Fashion"},
    {"id": "P002", "name": "Laptop ASUS TUF F15", "price": 12500000, "category": "Electronics"},
    {"id": "P003", "name": "Kopi Toraja 250gr", "price": 75000, "category": "Food"},
    {"id": "P004", "name": "Buku Clean Code", "price": 120000, "category": "Books"},
    {"id": "P005", "name": "Headphone Sony WH", "price": 1200000, "category": "Electronics"},
    {"id": "P006", "name": "Kemeja Batik Pria", "price": 180000, "category": "Fashion"},
    {"id": "P007", "name": "Vitamin C 1000mg", "price": 45000, "category": "Health"},
    {"id": "P008", "name": "Minyak Goreng 2L", "price": 35000, "category": "Food"},
    {"id": "P009", "name": "Sepatu Formal Pria", "price": 450000, "category": "Fashion"},
    {"id": "P010", "name": "Smartphone Samsung A54", "price": 4500000, "category": "Electronics"},
    {"id": "P011", "name": "Tas Ransel Laptop", "price": 320000, "category": "Fashion"},
    {"id": "P012", "name": "Suplemen Whey Protein", "price": 380000, "category": "Health"},
]

CITIES = [
    "Jakarta", "Surabaya", "Bandung", "Medan", "Semarang",
    "Makassar", "Palembang", "Tangerang", "Depok", "Bekasi",
    "Bogor", "Yogyakarta", "Malang", "Bandar Lampung", "Padang"
]

def get_hour_weight(hour: int) -> int:
    """
    Returns a probability weight for a given hour to simulate 
    realistic daily traffic spikes (e.g., peak at 10 AM and 7 PM).
    """
    weights = {
        0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 2,
        6: 4, 7: 6, 8: 8, 9: 9, 10: 10, 11: 10,
        12: 9, 13: 8, 14: 8, 15: 8, 16: 9, 17: 10,
        18: 10, 19: 10, 20: 9, 21: 8, 22: 6, 23: 4
    }
    return weights.get(hour, 5)

def generate_event(hours_ago: float = 0):
    """
    Generates a single synthetic e-commerce event with localized data.
    """
    event_time = datetime.now() - timedelta(hours=hours_ago)
    
    # Weighted selection for event types to mimic funnel conversion
    event_type = random.choices(
        ["page_view", "add_to_cart", "checkout", "payment_success", "payment_failed"],
        weights=[50, 25, 12, 10, 3]
    )[0]

    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5) if event_type != "page_view" else None
    total = product["price"] * quantity if quantity else None

    return {
        "event_id": fake.uuid4(),
        "event_type": event_type,
        "timestamp": event_time.isoformat(),
        "session_id": fake.uuid4(),
        "device": random.choice(["mobile", "mobile", "mobile", "desktop", "tablet"]),
        "platform": random.choice(["android", "android", "ios", "web"]),
        "user": {
            "user_id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "city": random.choice(CITIES),
            "age": random.randint(18, 55),
        },
        "product": product,
        "quantity": quantity,
        "total_amount": total,
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )

    print(f"Starting bulk simulation: {TARGET_EVENTS:,} events")
    print("Scenario: 7 days of historical Indonesian traffic data\n")

    batch_size = 100
    try:
        for i in range(0, TARGET_EVENTS, batch_size):
            # Simulate random time within the last 7 days (168 hours)
            hours_ago = random.uniform(0, 168)
            count = min(batch_size, TARGET_EVENTS - i)

            for _ in range(count):
                event = generate_event(hours_ago=hours_ago)
                producer.send(TOPIC, value=event)

            producer.flush()
            
            # Progress tracking
            progress = (i + count) / TARGET_EVENTS * 100
            sys.stdout.write(f"\rProgress: {i+count:,}/{TARGET_EVENTS:,} events ({progress:.0f}%)")
            sys.stdout.flush()

        print(f"\n\nSuccess! Simulated traffic successfully pushed to Kafka.")
        
    except KeyboardInterrupt:
        print("\nSimulation interrupted by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()