import json

from confluent_kafka import Consumer, Producer
import psycopg2


# -----------------------------
# Redpanda Consumer
# -----------------------------
consumer_config = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "order-consumer-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)


# -----------------------------
# Redpanda DLQ Producer
# -----------------------------
dlq_producer_config = {
    "bootstrap.servers": "localhost:19092"
}

dlq_producer = Producer(dlq_producer_config)

ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"


# -----------------------------
# PostgreSQL
# -----------------------------
db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce",
    "user": "user",
    "password": "password"
}

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Connected to PostgreSQL.")
except Exception as e:
    print(f"PostgreSQL connection failed: {e}")
    raise


# -----------------------------
# Validation
# -----------------------------
def validate_order(order):
    required_fields = [
        "order_id",
        "customer_id",
        "product",
        "category",
        "quantity",
        "price",
        "city",
        "timestamp"
    ]

    # Check required fields
    for field in required_fields:
        if field not in order or order[field] in (None, ""):
            return False, f"Missing field: {field}"

    # Check quantity
    if not isinstance(order["quantity"], int) or order["quantity"] <= 0:
        return False, "Quantity must be a positive integer"

    # Check price
    if not isinstance(order["price"], (int, float)) or order["price"] <= 0:
        return False, "Price must be a positive number"

    return True, None


# -----------------------------
# Subscribe
# -----------------------------
consumer.subscribe([ORDERS_TOPIC])

print("Consumer started...")
print("Waiting for orders...\n")


try:
    while True:

        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        try:
            order = json.loads(
                message.value().decode("utf-8")
            )
        except json.JSONDecodeError:
            print("Invalid JSON detected. Sending to DLQ.")

            dlq_producer.produce(
                DLQ_TOPIC,
                value=message.value()
            )
            dlq_producer.flush()

            continue

        # Validate order
        valid, error_reason = validate_order(order)

        if not valid:

            print(
                f"INVALID ORDER: "
                f"{order.get('order_id', 'UNKNOWN')} "
                f"| Reason: {error_reason}"
            )

            # Add error information
            dlq_record = {
                "error": error_reason,
                "original_order": order
            }

            dlq_producer.produce(
                DLQ_TOPIC,
                value=json.dumps(dlq_record)
            )

            dlq_producer.flush()

            print("→ Sent to orders-dlq\n")

            continue

        # -----------------------------
        # Store valid order
        # -----------------------------
        insert_query = """
            INSERT INTO orders
            (
                order_id,
                customer_id,
                product,
                category,
                quantity,
                price,
                city,
                timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """

        cursor.execute(
            insert_query,
            (
                order["order_id"],
                order["customer_id"],
                order["product"],
                order["category"],
                order["quantity"],
                order["price"],
                order["city"],
                order["timestamp"]
            )
        )

        conn.commit()

        print(
            f"Stored order: "
            f"{order['order_id']} | "
            f"{order['product']} | "
            f"₹{order['price']}"
        )

        dlq_producer.poll(0)


except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
    cursor.close()
    conn.close()
    print("Connections closed.")
