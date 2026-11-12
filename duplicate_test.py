import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

now = datetime.now(timezone.utc).isoformat()

test_trade = {
    "source": "Coinbase",
    "product": "BTC-USD",
    "price": 75000.0,
    "size": 0.10,
    "side": "BUY",
    "trade_id": "DUPLICATE-TEST-001",
    "trade_time": now,
    "received_at": now
}

print("Sending the same trade twice...")

producer.send("crypto-trades", value=test_trade).get()
producer.send("crypto-trades", value=test_trade).get()

producer.flush()
producer.close()

print("Both test messages sent successfully.")
