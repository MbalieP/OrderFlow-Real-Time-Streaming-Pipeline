"""
Spark Order Consumer
Consumes order messages from Kafka and displays them in the console

Useful for testing/debugging the Kafka producer and verifying message flow.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'orders')

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaOrderConsumer") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint-consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # Reduce Spark verbosity

# Updated schema to match new producer format
order_schema = StructType() \
    .add("customer_id", IntegerType()) \
    .add("order_id", StringType()) \
    .add("product_name", StringType()) \
    .add("product_category", StringType()) \
    .add("unit_price", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("total_price", DoubleType()) \
    .add("order_timestamp", StringType())

print(f"📡 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
print(f"📬 Subscribing to topic: {KAFKA_TOPIC}")

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON messages
json_df = df.selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_timestamp")

orders_df = json_df.select(
    from_json(col("json"), order_schema).alias("data"),
    col("kafka_timestamp")
).select("data.*", "kafka_timestamp")

print("🔄 Starting to consume messages...")
print("Press Ctrl+C to stop\n")

# Write to console
query = orders_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .start()

query.awaitTermination()
