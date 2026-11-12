from kafka import KafkaConsumer
import json
import time
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

TOPIC = "crypto-trades"
THRESHOLD = 5000

DB_FILE = Path("results/crypto_trades.db")
OUTPUT_FILE = Path("results/live_trades.jsonl")

OUTPUT_FILE.parent.mkdir(exist_ok=True)

db = sqlite3.connect(DB_FILE)
db.execute("""
CREATE TABLE IF NOT EXISTS trades (
    product TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    trade_time TEXT NOT NULL,
    price REAL NOT NULL,
    size REAL NOT NULL,
    trade_value REAL NOT NULL,
    side TEXT,
    status TEXT NOT NULL,
    latency_ms REAL,
    throughput REAL,
    received_at TEXT,
    processed_at TEXT,
    PRIMARY KEY (product, trade_id)
)
""")
db.commit()

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    group_id="crypto-dashboard-consumer",
    value_deserializer=lambda data: json.loads(data.decode("utf-8"))
)

print("SQLite-enabled dashboard collector started...")
print("Waiting for live Coinbase trades...")

total = 0
duplicates = 0
start_time = time.time()

try:
    for message in consumer:
        trade = message.value

        if trade.get("source") != "Coinbase":
            continue

        product = trade.get("product")
        trade_id = str(trade.get("trade_id"))

        if not product or not trade_id:
            print("Skipping trade without unique identifier.")
            continue

        price = float(trade["price"])
        size = float(trade["size"])
        value = price * size

        received = datetime.fromisoformat(trade["received_at"])
        processed = datetime.now(timezone.utc)
        latency = (processed - received).total_seconds() * 1000

        elapsed = time.time() - start_time
        throughput = (total + 1) / elapsed if elapsed > 0 else 0

        status = (
            "ANOMALY DETECTED"
            if value >= THRESHOLD else "NORMAL"
        )

        record = {
            **trade,
            "trade_value": value,
            "status": status,
            "latency_ms": latency,
            "throughput": throughput,
            "processed_at": processed.isoformat()
        }

        cursor = db.execute("""
            INSERT OR IGNORE INTO trades (
                product, trade_id, trade_time,
                price, size, trade_value, side,
                status, latency_ms, throughput,
                received_at, processed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            product, trade_id, trade["trade_time"],
            price, size, value, trade.get("side"),
            status, latency, throughput,
            trade["received_at"], processed.isoformat()
        ))

        if cursor.rowcount == 0:
            duplicates += 1
            print(f"Duplicate skipped: {trade_id}")
            continue

        db.commit()
        total += 1

        with OUTPUT_FILE.open("a") as file:
            file.write(json.dumps(record) + "\n")

        print(
            f"Trade ID: {trade_id} | "
            f"Value: ${value:.2f} | "
            f"{status} | "
            f"Latency: {latency:.2f} ms"
        )

except KeyboardInterrupt:
    print("\nDashboard collector stopped.")

finally:
    print(f"New unique trades processed: {total}")
    print(f"Duplicate trades skipped: {duplicates}")
    consumer.close()
    db.close()
