
import json
import time
import requests

from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration
PRODUCT = "BTC-USD"
TOPIC = "crypto-trades"

# Original checkpoint before the recovery gap
CHECKPOINT_ID = 1119422822

HISTORY_URL = (
    "https://api.exchange.coinbase.com/products/"
    "BTC-USD/trades"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)


def recover_trades():

    print("Starting historical trade recovery...")
    print("Original checkpoint:", CHECKPOINT_ID)

    recovered = []
    cursor = None
    checkpoint_found = False
    max_pages = 1000

    try:
        for page in range(1, max_pages + 1):

            params = {"limit": 100}

            if cursor:
                params["after"] = cursor

            response = requests.get(
                HISTORY_URL,
                params=params,
                timeout=20
            )

            response.raise_for_status()

            trades = response.json()

            if not trades:
                print("No more historical trades available.")
                break

            print(
                f"Page {page}: received {len(trades)} trades"
            )

            for trade in trades:

                trade_id = int(trade["trade_id"])

                if trade_id <= CHECKPOINT_ID:
                    checkpoint_found = True
                    break

                recovered.append(trade)

            if checkpoint_found:
                print("Original checkpoint reached.")
                break

            next_cursor = response.headers.get("CB-AFTER")

            if not next_cursor or next_cursor == cursor:
                print("Pagination cursor unavailable or unchanged.")
                break

            cursor = next_cursor
            time.sleep(0.2)

        print("\nTotal historical records found:", len(recovered))

        recovered.sort(
            key=lambda trade: int(trade["trade_id"])
        )

        if not checkpoint_found:
            print(
                "WARNING: Original checkpoint was not reached."
            )
            print(
                "The available history may be incomplete."
            )

        print("Publishing recovered trades to Kafka...")

        published = 0

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
                TOPIC,
                value=trade_data
            ).get(timeout=20)

            published += 1

        producer.flush()

        print("Trades published to Kafka:", published)
        print("Checkpoint reached:", checkpoint_found)

        if checkpoint_found:
            print("Historical recovery completed.")
        else:
            print("Historical recovery is INCOMPLETE.")

    except Exception as error:
        print("Recovery error:", error)

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    recover_trades()
