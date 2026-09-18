
import json
import time
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# Kafka topic
topic = "aiobs.telemetry.raw"

# Send 100 JSON trace envelopes
for i in range(1, 101):

    trace = {
        "trace_id": f"trace-{i:03d}",
        "tenant_id": f"tenant-{(i % 3) + 1}",
        "service_name": "demo-service",
        "status": "success",
        "timestamp": time.time()
    }

    producer.send(topic, value=trace)

    print(f"Sent message {i}: {trace}")

# Make sure all messages are sent
producer.flush()

# Close the producer
producer.close()

print("Successfully sent 100 messages to Kafka.")

