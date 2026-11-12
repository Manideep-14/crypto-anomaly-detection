import requests
import sqlite3

url = "https://api.exchange.coinbase.com/products/BTC-USD/trades?limit=5"

response = requests.get(url, timeout=15)
response.raise_for_status()

historical_trades = response.json()

connection = sqlite3.connect("results/crypto_trades.db")

print("Historical API trade IDs and database matches:")
for trade in historical_trades:
    trade_id = str(trade["trade_id"])

    match = connection.execute(
        "SELECT COUNT(*) FROM trades WHERE product = ? AND trade_id = ?",
        ("BTC-USD", trade_id)
    ).fetchone()[0]

    print(
        f"ID: {trade_id} | "
        f"Price: ${trade['price']} | "
        f"Already in database: {'YES' if match else 'NO'}"
    )

connection.close()
