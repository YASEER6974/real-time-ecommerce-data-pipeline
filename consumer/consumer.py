import json
from datetime import datetime, timedelta, timezone

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

    for field in required_fields:
        if field not in order or order[field] in (None, ""):
            return False, f"Missing field: {field}"

    if not isinstance(order["quantity"], int) or order["quantity"] <= 0:
        return False, "Quantity must be a positive integer"

    if not isinstance(order["price"], (int, float)) or order["price"] <= 0:
        return False, "Price must be a positive number"

    return True, None


# -----------------------------
# 5-minute window calculation
# -----------------------------
def get_window_start(timestamp):
    dt = datetime.fromisoformat(timestamp)

    # Make sure timestamp is timezone-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    minutes = (dt.minute // 5) * 5

    return dt.replace(
        minute=minutes,
        second=0,
        microsecond=0
    )


# -----------------------------
# Analytics state
# -----------------------------
current_window_start = None
order_count = 0
total_revenue = 0.0


# -----------------------------
# Save completed analytics window
# -----------------------------
def save_window_metrics(window_start, count, revenue):
    if window_start is None or count == 0:
        return

    window_end = window_start + timedelta(minutes=5)
    average_order_value = revenue / count

    query = """
        INSERT INTO order_metrics
        (
            window_start,
            window_end,
            order_count,
            total_revenue,
            average_order_value
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (window_start)
        DO UPDATE SET
            window_end = EXCLUDED.window_end,
            order_count = EXCLUDED.order_count,
            total_revenue = EXCLUDED.total_revenue,
            average_order_value = EXCLUDED.average_order_value;
    """

    cursor.execute(
        query,
        (
            window_start,
            window_end,
            count,
            round(revenue, 2),
            round(average_order_value, 2)
        )
    )

    conn.commit()

    print(
        f"\nAnalytics window completed:"
        f"\n  Window: {window_start} → {window_end}"
        f"\n  Orders: {count}"
        f"\n  Revenue: ₹{revenue:.2f}"
        f"\n  Average Order Value: ₹{average_order_value:.2f}\n"
    )


# -----------------------------
# Subscribe to orders
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

        # -----------------------------
        # Parse JSON
        # -----------------------------
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

        # -----------------------------
        # Validate order
        # -----------------------------
        valid, error_reason = validate_order(order)

        if not valid:

            print(
                f"INVALID ORDER: "
                f"{order.get('order_id', 'UNKNOWN')} "
                f"| Reason: {error_reason}"
            )

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

        # -----------------------------
        # Analytics
        # -----------------------------
        order_timestamp = get_window_start(
            order["timestamp"]
        )

        # First order
        if current_window_start is None:
            current_window_start = order_timestamp

        # New window detected
        elif order_timestamp > current_window_start:

            save_window_metrics(
                current_window_start,
                order_count,
                total_revenue
            )

            current_window_start = order_timestamp
            order_count = 0
            total_revenue = 0.0

        # Update current window
        order_count += 1
        total_revenue += (
            order["price"] * order["quantity"]
        )

        dlq_producer.poll(0)


except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:

    # Save the final partially completed window
    save_window_metrics(
        current_window_start,
        order_count,
        total_revenue
    )

    consumer.close()
    cursor.close()
    conn.close()

    print("Connections closed.")
