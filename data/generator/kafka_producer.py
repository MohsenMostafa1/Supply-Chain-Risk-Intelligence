import json
import random
import time
from kafka import KafkaProducer
import datetime

KAFKA_TOPIC = "iot-sensor-data"
KAFKA_BROKER = "localhost:9092"

def create_producer():
    """Lazy initialisation of Kafka producer (to allow mocking in tests)."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# Do NOT create producer at module level
producer = None

def get_producer():
    global producer
    if producer is None:
        producer = create_producer()
    return producer

vehicle_ids = [f"vehicle_{i}" for i in range(1, 101)]

def generate_sensor_data():
    return {
        "vehicle_id": random.choice(vehicle_ids),
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(70, 120), 2),
        "vibration": round(random.uniform(0.1, 5.0), 3),
        "rpm": random.randint(500, 6000),
        "location": {
            "lat": round(random.uniform(40.0, 41.0), 6),
            "lon": round(random.uniform(-74.0, -73.0), 6)
        }
    }

if __name__ == "__main__":
    prod = get_producer()
    print("Starting Kafka producer... Press Ctrl+C to stop.")
    while True:
        data = generate_sensor_data()
        prod.send(KAFKA_TOPIC, value=data)
        print(f"Sent: {data['vehicle_id']} at {data['timestamp']}")
        time.sleep(1)
