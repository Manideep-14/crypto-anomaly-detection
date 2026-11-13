
import json
import time

import websocket

from datetime import datetime, timezone
from kafka import KafkaProducer


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

KAFKA_TOPIC = "crypto-trades"

COINBASE_URL = "wss://advanced-trade-ws.coinbase.com"

PRODUCT = "BTC-USD"

RETRY_SECONDS = 5


# --------------------------------------------------
# KAFKA PRODUCER
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)


# --------------------------------------------------
# LIVE WEBSOCKET CONNECTION
# --------------------------------------------------

def on_open(ws):

    print("\nConnected to Coinbase!")

    subscription = {
        "type": "subscribe",
        "product_ids": [PRODUCT],
        "channel": "market_trades"
    }

    ws.send(json.dumps(subscription))

    print("Subscribed to BTC-USD live trades.")
    print("Waiting for live trades...\n")


# --------------------------------------------------
# RECEIVE LIVE TRADES
# --------------------------------------------------

def on_message(ws, message):

    try:

        data = json.loads(message)

        if data.get("channel") != "market_trades":
            return

        for event in data.get("events", []):

            for trade in event.get("trades", []):

                trade_data = {
                    "source": "Coinbase",
                    "product": trade.get("product_id", PRODUCT),
                    "price": float(trade["price"]),
                    "size": float(trade["size"]),
                    "side": trade.get("side"),
                    "trade_id": str(trade["trade_id"]),
                    "trade_time": trade.get("time"),
                    "received_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "recovered": False
                }

                # Send trade to Kafka.
                producer.send(
                    KAFKA_TOPIC,
                    value=trade_data
                )

                print(
                    f"Live trade: {trade_data['trade_id']} | "
                    f"Price: ${trade_data['price']} | "
                    f"Size: {trade_data['size']}"
                )

    except Exception as error:

        print("Message processing error:", error)


# --------------------------------------------------
# WEBSOCKET ERROR HANDLING
# --------------------------------------------------

def on_error(ws, error):

    print("WebSocket error:", error)


def on_close(ws, close_status_code, close_message):

    print("\nCoinbase connection closed.")

    if close_status_code is not None:
        print("Close status:", close_status_code)


# --------------------------------------------------
# MAIN PROGRAM
# --------------------------------------------------

if __name__ == "__main__":

    print("----------------------------------")
    print("COINBASE LIVE TRADE PRODUCER")
    print("----------------------------------")

    print("Historical recovery: Disabled")
    print("Product:", PRODUCT)
    print("Kafka topic:", KAFKA_TOPIC)

    try:

        while True:

            print("\nConnecting to Coinbase...")

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

            print(
                f"\nReconnecting in {RETRY_SECONDS} seconds..."
            )

            time.sleep(RETRY_SECONDS)

    except KeyboardInterrupt:

        print("\nStopping producer...")

    finally:

        producer.flush()
        producer.close()

        print("Kafka producer closed.")
