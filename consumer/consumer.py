import json
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, Producer
import psycopg2


# ============================================================
# REDPANDA CONFIGURATION
# ============================================================

REDPANDA_SERVER = "localhost:19092"

ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"


# Main consumer
consumer_config = {
    "bootstrap.servers": REDPANDA_SERVER,
    "group.id": "order-consumer-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)


# Producer used to send failed events to DLQ
dlq_producer_config = {
    "bootstrap.servers": REDPANDA_SERVER
}

dlq_producer = Producer(dlq_producer_config)


# ============================================================
# POSTGRESQL CONFIGURATION
# ============================================================

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


# ============================================================
# ORDER VALIDATION
# ============================================================

def validate_order(order):
    """
    Validate incoming e-commerce order.
    Returns:
        (True, None) if valid
        (False, reason) if invalid
    """

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
    if (
        not isinstance(order["quantity"], int)
        or order["quantity"] <= 0
    ):
        return False, "Quantity must be a positive integer"

    # Check price
    if (
        not isinstance(order["price"], (int, float))
        or order["price"] <= 0
    ):
        return False, "Price must be a positive number"

    return True, None


# ============================================================
# 5-MINUTE WINDOW CALCULATION
# ============================================================

def get_window_start(timestamp):
    """
    Convert an event timestamp into the beginning
    of its 5-minute window.

    Example:
        13:37:42 → 13:35:00
        13:43:10 → 13:40:00
    """

    dt = datetime.fromisoformat(timestamp)

    # Make timestamp timezone-aware if necessary
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    minutes = (dt.minute // 5) * 5

    return dt.replace(
        minute=minutes,
        second=0,
        microsecond=0
    )


# ============================================================
# ANALYTICS STATE
# ============================================================

current_window_start = None
order_count = 0
total_revenue = 0.0


# ============================================================
# SAVE ANALYTICS WINDOW
# ============================================================

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
        "\nAnalytics window completed:"
        f"\n  Window: {window_start} → {window_end}"
        f"\n  Orders: {count}"
        f"\n  Revenue: ₹{revenue:.2f}"
        f"\n  Average Order Value: ₹{average_order_value:.2f}\n"
    )


# ============================================================
# SUBSCRIBE TO ORDERS TOPIC
# ============================================================

consumer.subscribe([ORDERS_TOPIC])

print("Consumer started...")
print("Waiting for orders...\n")


# ============================================================
# MAIN CONSUMER LOOP
# ============================================================

try:

    while True:

        message = consumer.poll(1.0)

        # No message available
        if message is None:
            continue

        # Kafka/Redpanda error
        if message.error():

            print(f"Consumer error: {message.error()}")

            continue

        # ====================================================
        # PARSE JSON
        # ====================================================

        try:

            order = json.loads(
                message.value().decode("utf-8")
            )

        except json.JSONDecodeError:

            print(
                "Invalid JSON detected. "
                "Sending raw message to DLQ."
            )

            # Send raw invalid message to DLQ
            dlq_producer.produce(
                DLQ_TOPIC,
                value=message.value()
            )

            dlq_producer.flush()

            # Record failure in PostgreSQL
            cursor.execute(
                """
                INSERT INTO dlq_events
                (
                    order_id,
                    error_reason
                )
                VALUES (%s, %s);
                """,
                (
                    None,
                    "Invalid JSON"
                )
            )

            conn.commit()

            print(
                "→ Sent invalid JSON to orders-dlq "
                "and recorded in dlq_events\n"
            )

            continue

        # ====================================================
        # VALIDATE ORDER
        # ====================================================

        valid, error_reason = validate_order(order)

        # ====================================================
        # INVALID ORDER
        # ====================================================

        if not valid:

            order_id = order.get(
                "order_id",
                "UNKNOWN"
            )

            print(
                f"INVALID ORDER: "
                f"{order_id} "
                f"| Reason: {error_reason}"
            )

            # Create detailed DLQ record
            dlq_record = {
                "error": error_reason,
                "original_order": order
            }

            # Send failed event to DLQ
            dlq_producer.produce(
                DLQ_TOPIC,
                value=json.dumps(dlq_record)
            )

            dlq_producer.flush()

            # ----------------------------------------------
            # Record DLQ event in PostgreSQL
            # ----------------------------------------------

            dlq_query = """
                INSERT INTO dlq_events
                (
                    order_id,
                    error_reason
                )
                VALUES (%s, %s);
            """

            cursor.execute(
                dlq_query,
                (
                    order.get("order_id"),
                    error_reason
                )
            )

            conn.commit()

            print(
                "→ Sent to orders-dlq "
                "and recorded in dlq_events\n"
            )

            # Do NOT store invalid order
            # Do NOT include it in analytics
            continue

        # ====================================================
        # VALID ORDER → STORE IN POSTGRESQL
        # ====================================================

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
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )

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

        # ====================================================
        # STREAMING ANALYTICS
        # ====================================================

        order_timestamp = get_window_start(
            order["timestamp"]
        )

        # First valid order
        if current_window_start is None:

            current_window_start = order_timestamp

        # Newer 5-minute window detected
        elif order_timestamp > current_window_start:

            # Save previous window
            save_window_metrics(
                current_window_start,
                order_count,
                total_revenue
            )

            # Start new window
            current_window_start = order_timestamp

            order_count = 0
            total_revenue = 0.0

        # Add valid order to current window
        order_count += 1

        total_revenue += (
            order["price"] * order["quantity"]
        )

        # Let DLQ delivery callbacks execute
        dlq_producer.poll(0)


# ============================================================
# CLEAN SHUTDOWN
# ============================================================

except KeyboardInterrupt:

    print("\nConsumer stopped by user.")


finally:

    # Save final partially completed window
    save_window_metrics(
        current_window_start,
        order_count,
        total_revenue
    )

    consumer.close()

    cursor.close()

    conn.close()

    print("Connections closed.")
