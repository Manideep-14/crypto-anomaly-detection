
import json
import time
import sqlite3
import requests
import websocket

from datetime import datetime, timezone
from kafka import KafkaProducer


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

KAFKA_TOPIC = "crypto-trades"

COINBASE_URL = "wss://advanced-trade-ws.coinbase.com"

HISTORY_URL = (
    "https://api.exchange.coinbase.com/products/BTC-USD/trades"
)

DB_FILE = "results/crypto_trades.db"

PRODUCT = "BTC-USD"

PAGE_LIMIT = 100
MAX_PAGES = 5000
RETRY_SECONDS = 10
REQUEST_DELAY = 0.15


# --------------------------------------------------
# KAFKA PRODUCER
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)


# --------------------------------------------------
# GET LAST PROCESSED TRADE ID
# --------------------------------------------------

def get_last_processed_trade_id():

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


# --------------------------------------------------
# HISTORICAL TRADE RECOVERY
# --------------------------------------------------

def recover_missed_trades():

    last_id = get_last_processed_trade_id()

    print("\n----------------------------------")
    print("HISTORICAL TRADE RECOVERY")
    print("----------------------------------")

    print(f"Last processed trade ID: {last_id}")

    # No previous checkpoint means there is no known outage gap.
    if last_id == 0:
        print("No previous checkpoint found.")
        print("Starting live streaming.")
        return True

    print("Checking Coinbase for missed trades...")

    recovered = []
    seen_ids = set()

    params = {"limit": PAGE_LIMIT}
    reached_checkpoint = False
    previous_cursor = None

    session = requests.Session()

    try:

        for page in range(1, MAX_PAGES + 1):

            response = session.get(
                HISTORY_URL,
                params=params,
                timeout=20
            )

            if response.status_code == 429:
                print("Coinbase rate limit reached. Waiting 5 seconds...")
                time.sleep(5)
                continue

            response.raise_for_status()

            trades = response.json()

            if not trades:
                raise RuntimeError(
                    "No more historical trades returned "
                    "before reaching the checkpoint."
                )

            new_count = 0

            for trade in trades:

                trade_id = int(trade["trade_id"])

                if trade_id <= last_id:
                    reached_checkpoint = True
                    continue

                if trade_id not in seen_ids:

                    recovered.append(trade)
                    seen_ids.add(trade_id)
                    new_count += 1

            print(
                f"Page {page}: "
                f"{len(trades)} received, "
                f"{new_count} new trades collected"
            )

            if reached_checkpoint:
                break

            cursor = response.headers.get("CB-AFTER")

            if not cursor:
                raise RuntimeError(
                    "Coinbase pagination cursor missing."
                )

            if cursor == previous_cursor:
                raise RuntimeError(
                    "Pagination cursor did not advance."
                )

            previous_cursor = cursor

            params = {
                "limit": PAGE_LIMIT,
                "after": cursor
            }

            time.sleep(REQUEST_DELAY)

        else:

            raise RuntimeError(
                f"Maximum page limit ({MAX_PAGES}) reached "
                "before reaching checkpoint."
            )

        if not reached_checkpoint:
            raise RuntimeError(
                "Recovery incomplete: checkpoint not reached."
            )

        # Publish recovered trades from oldest to newest.
        recovered.sort(
            key=lambda trade: int(trade["trade_id"])
        )

        print(f"\nMissed trades found: {len(recovered)}")

        for trade in recovered:

            trade_data = {
                "source": "Coinbase",
                "product": PRODUCT,
                "price": float(trade["price"]),
                "size": float(trade["size"]),
                "side": trade.get("side"),
                "trade_id": str(trade["trade_id"]),
                "trade_time": trade["time"],
                "received_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "recovered": True
            }

            producer.send(
                KAFKA_TOPIC,
                value=trade_data
            ).get(timeout=20)

        producer.flush()

        print("Historical recovery completed.")
        print("Saved checkpoint successfully reached.")

        return True

    except Exception as error:

        print("\nHistorical recovery error:", error)

        print("Live streaming will not start until recovery succeeds.")

        return False

    finally:

        session.close()


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
    print("COINBASE TRADE PRODUCER")
    print("----------------------------------")

    try:

        while True:

            recovery_success = recover_missed_trades()

            if not recovery_success:

                print(
                    f"\nRetrying recovery in "
                    f"{RETRY_SECONDS} seconds..."
                )

                time.sleep(RETRY_SECONDS)
                continue

            print("\nStarting live Coinbase streaming...")

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
