import unittest
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from pyspark.sql import SparkSession
import json

class TestSparkKafkaIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Wait for Kafka to become available (max 60 seconds)
        cls.producer = None
        start_time = time.time()
        while time.time() - start_time < 60:
            try:
                cls.producer = KafkaProducer(
                    bootstrap_servers="localhost:9092",
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                # If we get here, connection succeeded
                break
            except NoBrokersAvailable:
                print("Kafka not ready yet, waiting 2 seconds...")
                time.sleep(2)
        if cls.producer is None:
            raise RuntimeError("Could not connect to Kafka after 60 seconds")
        
        # Spark session (with Kafka package)
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("IntegrationTest") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .getOrCreate()

    def test_kafka_to_spark_stream(self):
        # Send a test message
        test_msg = {
            "vehicle_id": "test",
            "timestamp": "2025-01-01T00:00:00",
            "temperature": 100,
            "vibration": 2.5,
            "rpm": 3000,
            "location": {"lat": 40.0, "lon": -74.0}
        }
        future = self.producer.send("iot-sensor-data", value=test_msg)
        future.get(timeout=5)  # Wait for send to complete
        time.sleep(2)  # Allow message to be processed

        # Read from Kafka in Spark (batch mode for test)
        df = self.spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "iot-sensor-data") \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)")

        self.assertGreater(df.count(), 0)
        # Optionally check content
        row = df.first()
        self.assertIsNotNone(row)
        self.assertIn("vehicle_id", row.value)

    @classmethod
    def tearDownClass(cls):
        if cls.producer:
            cls.producer.close()
        if cls.spark:
            cls.spark.stop()

if __name__ == "__main__":
    unittest.main()
