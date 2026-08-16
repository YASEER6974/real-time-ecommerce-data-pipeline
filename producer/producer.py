import json
import time
import random
from datetime import datetime, timezone

from confluent_kafka import Producer


# Redpanda connection
config = {
    "bootstrap.servers": "localhost:19092"
}

producer = Producer(config)

TOPIC = "orders"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Order delivered to {msg.topic()} "
            f"[partition {msg.partition()}]"
        )


products = [
    ("Laptop", "Electronics", 55000),
    ("Headphones", "Electronics", 2500),
    ("Smartphone", "Electronics", 30000),
    ("Shoes", "Fashion", 3500),
    ("Backpack", "Fashion", 1800),
    ("Coffee Maker", "Home", 4500),
]


while True:

    product, category, price = random.choice(products)

    order = {
        "order_id": f"ORD{random.randint(10000, 99999)}",
        "customer_id": f"CUS{random.randint(1000, 9999)}",
        "product": product,
        "category": category,
        "quantity": random.randint(1, 3),
        "price": price,
        "city": random.choice(
            ["Mumbai", "Pune", "Delhi", "Bangalore", "Hyderabad"]
        ),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    message = json.dumps(order)

    producer.produce(
        TOPIC,
        value=message,
        callback=delivery_report
    )

    producer.poll(0)

    print(f"Produced: {message}")

    time.sleep(2)
