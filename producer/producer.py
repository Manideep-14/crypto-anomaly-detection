
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

message = {
    "type": "connection_test",
    "message": "Hello Kafka from Python!"
}

producer.send("crypto-trades", value=message)
producer.flush()

print("Message successfully sent to Kafka!")

producer.close()
