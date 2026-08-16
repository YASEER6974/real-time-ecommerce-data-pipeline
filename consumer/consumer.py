import json

from confluent_kafka import Consumer


config = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "order-consumer-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(config)

TOPIC = "orders"

consumer.subscribe([TOPIC])

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

        order = json.loads(message.value().decode("utf-8"))

        print(
            f"Received order: "
            f"{order['order_id']} | "
            f"{order['product']} | "
            f"₹{order['price']}"
        )

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
