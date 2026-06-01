import unittest
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import time

class TestSparkKafkaIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("IntegrationTest") \
            .getOrCreate()
        cls.producer = KafkaProducer(bootstrap_servers="localhost:9092",
                                     value_serializer=lambda v: json.dumps(v).encode())

    def test_kafka_to_spark_stream(self):
        # Send a test message
        test_msg = {"vehicle_id": "test", "timestamp": "2025-01-01T00:00:00",
                    "temperature": 100, "vibration": 2.5, "rpm": 3000, "location": {"lat": 40.0, "lon": -74.0}}
        self.producer.send("iot-sensor-data", value=test_msg)
        time.sleep(2)

        # Read from Kafka in Spark (batch mode for test)
        df = self.spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "iot-sensor-data") \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)")

        self.assertGreater(df.count(), 0)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == "__main__":
    unittest.main()
