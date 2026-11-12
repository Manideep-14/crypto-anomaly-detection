import sqlite3
import json
from pathlib import Path

JSON_FILE = Path("results/live_trades.jsonl")
DB_FILE = Path("results/crypto_trades.db")

connection = sqlite3.connect(DB_FILE)

connection.execute("""
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

connection.commit()

inserted = 0
duplicates = 0

if JSON_FILE.exists():
    with JSON_FILE.open() as file:
        for line in file:
            try:
                trade = json.loads(line)

                cursor = connection.execute("""
                    INSERT OR IGNORE INTO trades (
                        product, trade_id, trade_time,
                        price, size, trade_value, side,
                        status, latency_ms, throughput,
                        received_at, processed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trade["product"],
                    str(trade["trade_id"]),
                    trade["trade_time"],
                    trade["price"],
                    trade["size"],
                    trade["trade_value"],
                    trade.get("side"),
                    trade["status"],
                    trade.get("latency_ms"),
                    trade.get("throughput"),
                    trade.get("received_at"),
                    trade.get("processed_at")
                ))

                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    duplicates += 1

            except (json.JSONDecodeError, KeyError, TypeError) as error:
                print("Skipped invalid record:", error)

connection.commit()

total = connection.execute(
    "SELECT COUNT(*) FROM trades"
).fetchone()[0]

connection.close()

print("Database setup completed!")
print("New historical trades imported:", inserted)
print("Duplicate records skipped:", duplicates)
print("Total unique trades in database:", total)
print("Database location:", DB_FILE)
