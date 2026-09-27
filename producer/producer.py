import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]

def next_event():
    return {
        "order_id": str(uuid.uuid4()),
        "trader_id": f"trader-{random.randint(1, 20)}",
        "symbol": random.choice(SYMBOLS),
        "side": random.choice(["BUY", "SELL"]),
        "order_type": random.choice(["LIMIT", "MARKET", "STOP"]),
        "quantity": random.randint(1, 50) * 10,
        "price": round(random.uniform(100, 500), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

def main():
    topic = "orders"

    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        "acks": "all",
        "enable.idempotence": True,
    })

    sent = 0
    try:
        while True:
            event = next_event()
            producer.produce(
                topic,
                value=json.dumps(event).encode(),
            )
            producer.poll(0)
            sent += 1
            print(json.dumps(event))
            time.sleep(1) # 1 event per second
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"Sent {sent} events to '{topic}'")

if __name__ == "__main__":
    main()
