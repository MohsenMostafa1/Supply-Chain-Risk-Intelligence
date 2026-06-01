"""
Reads raw Parquet from HDFS, aggregates, and writes to PostgreSQL.
Run with: spark-submit --jars postgresql-42.5.0.jar hdfs_to_postgres.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, to_date

spark = SparkSession.builder \
    .appName("HDFS_to_Postgres_ETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Read raw IoT data from HDFS (Parquet)
df_raw = spark.read.parquet("hdfs://localhost:9000/user/raw_iot/*.parquet")

# Daily aggregation per vehicle
df_daily = df_raw \
    .withColumn("date", to_date(col("timestamp"))) \
    .groupBy("vehicle_id", "date") \
    .agg(
        avg("temperature").alias("avg_temp_daily"),
        avg("vibration").alias("avg_vib_daily"),
        avg("rpm").alias("avg_rpm_daily"),
        count("*").alias("readings_count")
    )

# Write to PostgreSQL (replace with your JDBC URL)
df_daily.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/orion") \
    .option("dbtable", "daily_aggregates") \
    .option("user", "admin") \
    .option("password", "admin123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()
