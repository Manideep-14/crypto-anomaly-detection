
import json
import time
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer

KAFKA_TOPIC = "crypto-trades"
COINBASE_URL = "wss://advanced-trade-ws.coinbase.com"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)


def on_open(ws):
    print("Connected to Coinbase!")

    subscription = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channel": "market_trades"
    }

    ws.send(json.dumps(subscription))
    print("Subscribed to BTC-USD live trades")


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
                    "trade_id": trade.get("trade_id"),
                    "trade_time": trade.get("time"),
                    "received_at": datetime.now(timezone.utc).isoformat()
                }

                producer.send(KAFKA_TOPIC, value=trade_data)

                print("Live trade:", trade_data)

    except Exception as error:
        print("Message processing error:", error)


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws, close_status_code, close_message):
    print("Coinbase connection closed")


if __name__ == "__main__":
    print("Starting Coinbase live trade producer...")

    ws = websocket.WebSocketApp(
        COINBASE_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    try:
        ws.run_forever(ping_interval=20, ping_timeout=10)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()
        print("Kafka producer closed")
