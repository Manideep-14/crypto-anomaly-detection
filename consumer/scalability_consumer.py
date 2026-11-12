from kafka import KafkaConsumer
import json
import time
import sys

GROUP_ID = "scalability-test"
TOPIC = "crypto-trades"

consumer_name = sys.argv[1] if len(sys.argv) > 1 else "consumer"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    group_id=GROUP_ID,
    value_deserializer=lambda data: json.loads(data.decode("utf-8"))
)

print(f"{consumer_name} started. Waiting for live trades...")

total_trades = 0
start_time = time.time()

try:
    for message in consumer:
        trade = message.value

        if trade.get("source") != "Coinbase":
            continue

        total_trades += 1
        elapsed = time.time() - start_time
        throughput = total_trades / elapsed if elapsed > 0 else 0

        print(
            f"{consumer_name} | "
            f"Trades: {total_trades} | "
            f"Throughput: {throughput:.2f} trades/sec"
        )

except KeyboardInterrupt:
    print(f"\n{consumer_name} stopped.")

finally:
    elapsed = time.time() - start_time
    print("\n--- Scalability Test Summary ---")
    print(f"Consumer: {consumer_name}")
    print(f"Total trades: {total_trades}")
    print(f"Elapsed time: {elapsed:.2f} seconds")

    if elapsed > 0:
        print(f"Average throughput: {total_trades / elapsed:.2f} trades/sec")

    consumer.close()
