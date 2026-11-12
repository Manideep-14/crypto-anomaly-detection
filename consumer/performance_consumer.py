from kafka import KafkaConsumer
import json
import time
from datetime import datetime, timezone

consumer = KafkaConsumer(
    "crypto-trades",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    group_id="clean-performance-test",
    enable_auto_commit=False,
    value_deserializer=lambda data: json.loads(data.decode("utf-8"))
)

print("Clean performance test started. Waiting for new trades...")

total = 0
latencies = []
start = time.time()

try:
    for message in consumer:
        trade = message.value

        if trade.get("source") != "Coinbase":
            continue

        total += 1
        received = datetime.fromisoformat(trade["received_at"])
        now = datetime.now(timezone.utc)
        latency = (now - received).total_seconds() * 1000
        latencies.append(latency)

        elapsed = time.time() - start
        throughput = total / elapsed if elapsed > 0 else 0

        value = trade["price"] * trade["size"]
        status = "ANOMALY DETECTED" if value >= 5000 else "NORMAL"

        print(
            f"Value: ${value:.2f} | {status} | "
            f"Latency: {latency:.2f} ms | "
            f"Throughput: {throughput:.2f} trades/sec"
        )

except KeyboardInterrupt:
    print("\nTest stopped.")

finally:
    elapsed = time.time() - start
    print("\n--- Clean Performance Summary ---")
    print(f"Total trades: {total}")
    print(f"Duration: {elapsed:.2f} seconds")

    if elapsed > 0:
        print(f"Average throughput: {total / elapsed:.2f} trades/sec")

    if latencies:
        print(f"Average latency: {sum(latencies) / len(latencies):.2f} ms")
        print(f"Maximum latency: {max(latencies):.2f} ms")

    consumer.close()
