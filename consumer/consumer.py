
from kafka import KafkaConsumer
import json
import time
from datetime import datetime, timezone

THRESHOLD = 5000

consumer = KafkaConsumer(
    "crypto-trades",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    group_id="crypto-anomaly-detector",
    value_deserializer=lambda data: json.loads(data.decode("utf-8"))
)

print("Real-Time Crypto Anomaly Detection Started")
print("------------------------------------------")

total_trades = 0
start_time = time.time()
latencies = []

try:
    for message in consumer:
        trade = message.value

        if trade.get("source") != "Coinbase":
            continue

        price = trade["price"]
        size = trade["size"]
        trade_value = price * size

        if trade_value >= THRESHOLD:
            status = "ANOMALY DETECTED"
        else:
            status = "NORMAL"

        # Measure consumer processing throughput
        total_trades += 1
        elapsed_time = time.time() - start_time
        throughput = total_trades / elapsed_time if elapsed_time > 0 else 0

        # Measure time from producer receipt to consumer processing
        received_at = datetime.fromisoformat(trade["received_at"])
        processing_time = datetime.now(timezone.utc)
        latency_ms = (processing_time - received_at).total_seconds() * 1000
        latencies.append(latency_ms)

        print(
            f"Price: ${price:.2f} | "
            f"Size: {size} BTC | "
            f"Value: ${trade_value:.2f} | "
            f"Status: {status} | "
            f"Latency: {latency_ms:.2f} ms | "
            f"Throughput: {throughput:.2f} trades/sec"
        )

except KeyboardInterrupt:
    print("\nDetection stopped.")

finally:
    elapsed_time = time.time() - start_time

    print("\n--- Performance Summary ---")
    print(f"Total trades processed: {total_trades}")

    if elapsed_time > 0:
        print(f"Average throughput: {total_trades / elapsed_time:.2f} trades/sec")

    if latencies:
        print(f"Average latency: {sum(latencies) / len(latencies):.2f} ms")
        print(f"Maximum latency: {max(latencies):.2f} ms")

    consumer.close()

