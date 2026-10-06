import json
import os
from datetime import datetime, timezone

import mysql.connector
from confluent_kafka import Consumer

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS orders (
  order_id      VARCHAR(64) PRIMARY KEY,
  trader_id     VARCHAR(32),
  symbol        VARCHAR(8),
  side          VARCHAR(8),
  order_type    VARCHAR(8),
  quantity      INT,
  price         DECIMAL(10,2),
  event_time    DATETIME(6),
  received_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  INDEX (symbol, event_time)
)
"""

INSERT = """
INSERT IGNORE INTO orders
  (order_id, trader_id, symbol, side, order_type, quantity, price, event_time)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

def main():
    db = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "cis641"),
        password=os.getenv("MYSQL_PASSWORD", "cis641"),
        database=os.getenv("MYSQL_DATABASE", "exchange"),
    )
    cursor = db.cursor()
    cursor.execute(CREATE_TABLE)

    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        "group.id": "order-recorder",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False, # commit only after MySQL has the order
    })
    consumer.subscribe(["orders"])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            order = json.loads(msg.value())
            # MySQL DATETIME has no timezone, so store as naive UTC
            event_time = datetime.fromisoformat(order["timestamp"]).astimezone(timezone.utc).replace(tzinfo=None)

            cursor.execute(INSERT, (
                order["order_id"], order["trader_id"], order["symbol"], order["side"],
                order["order_type"], order["quantity"], order["price"], event_time,
            ))
            db.commit()
            consumer.commit(msg, asynchronous=False)
            print(f"Stored {order['order_id']}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        db.close()

if __name__ == "__main__":
    main()
