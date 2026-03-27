from faker import Faker
import json
import random
import time
from datetime import datetime

# --- Mock Data & Constants ---
fake = Faker('id_ID')

PRODUCTS = [
    {"id": "P001", "name": "Sepatu Lari Nike", "price": 850000, "category": "Fashion"},
    {"id": "P002", "name": "Laptop ASUS TUF F15", "price": 12500000, "category": "Electronics"},
    {"id": "P003", "name": "Kopi Toraja 250gr", "price": 75000, "category": "Food"},
    {"id": "P004", "name": "Buku Clean Code", "price": 120000, "category": "Books"},
    {"id": "P005", "name": "Headphone Sony WH", "price": 1200000, "category": "Electronics"},
    {"id": "P006", "name": "Kemeja Batik Pria", "price": 180000, "category": "Fashion"},
    {"id": "P007", "name": "Vitamin C 1000mg", "price": 45000, "category": "Health"},
    {"id": "P008", "name": "Minyak Goreng 2L", "price": 35000, "category": "Food"},
]

def generate_user() -> dict:
    """
    Generates a synthetic user profile with Indonesian localization.
    """
    return {
        "user_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "age": random.randint(18, 55),
    }

def generate_event(user: dict) -> dict:
    """
    Generates a synthetic e-commerce event for a given user.
    """
    product = random.choice(PRODUCTS)
    
    # Simulating funnel: Most events are page views, fewer are successful payments
    event_type = random.choices(
        ["page_view", "add_to_cart", "checkout", "payment_success", "payment_failed"],
        weights=[50, 25, 12, 10, 3]
    )[0]

    # Logic: quantity and amount are only relevant for conversion-type events
    quantity = random.randint(1, 5) if event_type != "page_view" else None
    total_amount = (product["price"] * quantity) if quantity else None

    return {
        "event_id": fake.uuid4(),
        "event_type": event_type,
        "timestamp": datetime.now().isoformat(),
        "user": user,
        "product": product,
        "quantity": quantity,
        "total_amount": total_amount,
        "session_id": fake.uuid4(),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "platform": random.choice(["android", "ios", "web"]),
    }

if __name__ == "__main__":
    print("GlowCart event generator initialized...")
    print("Running sample output (10 events):\n")

    try:
        for i in range(1, 11):
            user = generate_user()
            event = generate_event(user)
            
            # Formatted log for terminal monitoring
            print(f"[{i}] {event['timestamp']} | {event['event_type']} | {event['user']['name']} | {event['product']['name']}")
            
            # Uncomment for full JSON debugging
            # print(json.dumps(event, indent=2, ensure_ascii=False))
            
            time.sleep(0.3)
    except KeyboardInterrupt:
        print("\nGenerator stopped.")

    print("\nCompleted generating samples.")
