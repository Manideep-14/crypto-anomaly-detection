
import json
import time
import sqlite3
import requests
import websocket

from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_TOPIC = "crypto-trades"
COINBASE_URL = "wss://advanced-trade-ws.coinbase.com"
HISTORY_URL = "https://api.exchange.coinbase.com/products/BTC-USD/trades"

DB_FILE = "results/crypto_trades.db"
PRODUCT = "BTC-USD"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)


def get_last_processed_trade_id():
    """Read the latest trade already stored by the consumer."""
    connection = sqlite3.connect(DB_FILE)

    try:
        result = connection.execute(
            """
            SELECT MAX(CAST(trade_id AS INTEGER))
            FROM trades
            WHERE product = ?
            """,
            (PRODUCT,)
        ).fetchone()

        return int(result[0]) if result[0] is not None else 0

    finally:
        connection.close()


def recover_missed_trades():
    """Retrieve available historical trades after the last stored trade."""
    last_id = get_last_processed_trade_id()

    print(f"\nLast processed trade ID: {last_id}")
    print("Checking Coinbase for missed trades...")

    recovered = []
    params = {"limit": 100}
    reached_checkpoint = False

    try:
        for page in range(1000):
            response = requests.get(
                HISTORY_URL,
                params=params,
                timeout=15
            )
            response.raise_for_status()

            trades = response.json()

            if not trades:
                break

            for trade in trades:
                trade_id = int(trade["trade_id"])

                if trade_id <= last_id:
                    reached_checkpoint = True
                else:
                    recovered.append(trade)

            if reached_checkpoint:
                break

            cursor = response.headers.get("CB-AFTER")

            if not cursor:
                break

            params = {
                "limit": 100,
                "after": cursor
            }

            time.sleep(0.1)

        recovered.sort(key=lambda trade: int(trade["trade_id"]))

        print(f"Missed trades found: {len(recovered)}")

        for trade in recovered:
            trade_data = {
                "source": "Coinbase",
                "product": PRODUCT,
                "price": float(trade["price"]),
                "size": float(trade["size"]),
                "side": trade.get("side"),
                "trade_id": str(trade["trade_id"]),
                "trade_time": trade["time"],
                "received_at": datetime.now(timezone.utc).isoformat(),
                "recovered": True
            }

            producer.send(
                KAFKA_TOPIC,
                value=trade_data
            ).get(timeout=10)

        producer.flush()

        if reached_checkpoint:
         print("Historical recovery completed. Checkpoint reached.")
        else:
         print("WARNING: Recovery incomplete. Checkpoint not reached.")

    except Exception as error:
        print("Historical recovery error:", error)
        print("The producer will retry after reconnecting.")


def on_open(ws):
    print("\nConnected to Coinbase!")

    subscription = {
        "type": "subscribe",
        "product_ids": [PRODUCT],
        "channel": "market_trades"
    }

    ws.send(json.dumps(subscription))
    print("Subscribed to BTC-USD live trades.")


def on_message(ws, message):
    try:
        data = json.loads(message)

        if data.get("channel") != "market_trades":
            return

        for event in data.get("events", []):
            for trade in event.get("trades", []):

                trade_data = {
                    "source": "Coinbase",
                    "product": trade.get("product_id"),
                    "price": float(trade["price"]),
                    "size": float(trade["size"]),
                    "side": trade.get("side"),
                    "trade_id": str(trade["trade_id"]),
                    "trade_time": trade.get("time"),
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "recovered": False
                }

                producer.send(
                    KAFKA_TOPIC,
                    value=trade_data
                )

                print(
                    "Live trade:",
                    trade_data["trade_id"],
                    trade_data["price"]
                )

    except Exception as error:
        print("Message processing error:", error)


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws, close_status_code, close_message):
    print("Coinbase connection closed.")


if __name__ == "__main__":

    print("Starting Coinbase trade producer...")

    try:
        while True:

            recover_missed_trades()

            ws = websocket.WebSocketApp(
                COINBASE_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            try:
                ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10
                )

            except KeyboardInterrupt:
                raise

            except Exception as error:
                print("Connection error:", error)

            print("\nReconnecting to Coinbase in 5 seconds...")
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        producer.close()
        print("Kafka producer closed.")

