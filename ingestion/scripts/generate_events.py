from faker import Faker
import json
import random
import time
from datetime import datetime

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


def generate_user():
    return {
        "user_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "age": random.randint(18, 55),
    }


def generate_event(user):
    product = random.choice(PRODUCTS)
    event_type = random.choices(
        ["page_view", "add_to_cart", "checkout", "payment_success", "payment_failed"],
        weights=[50, 25, 12, 10, 3]
    )[0]

    return {
        "event_id": fake.uuid4(),
        "event_type": event_type,
        "timestamp": datetime.now().isoformat(),
        "user": user,
        "product": product,
        "quantity": random.randint(1, 5) if event_type != "page_view" else None,
        "total_amount": product["price"] * random.randint(1, 5) if event_type != "page_view" else None,
        "session_id": fake.uuid4(),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "platform": random.choice(["android", "ios", "web"]),
    }


if __name__ == "__main__":
    print("GlowCart event generator started...")
    print("Generating e-commerce events:\n")

    for i in range(10):
        user = generate_user()
        event = generate_event(user)
        print(json.dumps(event, indent=2, ensure_ascii=False))
        print("-" * 50)
        time.sleep(0.5)

    print("\nDone! Generated 10 events.")
