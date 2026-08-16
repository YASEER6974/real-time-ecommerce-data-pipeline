import json
import time
import random
from datetime import datetime, timezone

from confluent_kafka import Producer


# -----------------------------
# Redpanda configuration
# -----------------------------
config = {
    "bootstrap.servers": "localhost:19092"
}

producer = Producer(config)

TOPIC = "orders"


# -----------------------------
# Delivery confirmation
# -----------------------------
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Order delivered to {msg.topic()} "
            f"[partition {msg.partition()}]"
        )


# -----------------------------
# Product catalogue
# -----------------------------
products = [
    ("Laptop", "Electronics", 55000),
    ("Headphones", "Electronics", 2500),
    ("Smartphone", "Electronics", 30000),
    ("Shoes", "Fashion", 3500),
    ("Backpack", "Fashion", 1800),
    ("Coffee Maker", "Home", 4500),
]


# -----------------------------
# Generate orders continuously
# -----------------------------
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

    # ---------------------------------
    # Generate invalid event occasionally
    # ---------------------------------
    if random.random() < 0.10:

        invalid_type = random.choice([
            "negative_price",
            "zero_quantity",
            "missing_customer",
            "missing_product"
        ])

        if invalid_type == "negative_price":
            order["price"] = -500

        elif invalid_type == "zero_quantity":
            order["quantity"] = 0

        elif invalid_type == "missing_customer":
            order["customer_id"] = None

        elif invalid_type == "missing_product":
            order["product"] = ""

        print(
            f"\n⚠️ Generated INVALID order "
            f"({invalid_type}): {order}"
        )

    else:
        print(f"\nGenerated VALID order: {order}")

    # Convert Python dictionary to JSON
    message = json.dumps(order)

    # Send event to Redpanda
    producer.produce(
        TOPIC,
        value=message,
        callback=delivery_report
    )

    # Allow delivery callback to execute
    producer.poll(0)

    time.sleep(2)
