"""
Spark to MySQL Consumer
Consumes order messages from Kafka and writes them to MySQL

Features:
- Environment variable configuration
- Error handling and logging
- Updated schema matching new producer format
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'orders')
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'ecommerce')
MYSQL_TABLE = os.getenv('MYSQL_TABLE', 'orders')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')

# Build MySQL JDBC URL
mysql_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaToMySQL") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint-mysql") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # Reduce Spark verbosity

# Updated schema to match new producer format
schema = StructType() \
    .add("customer_id", IntegerType()) \
    .add("order_id", StringType()) \
    .add("product_name", StringType()) \
    .add("product_category", StringType()) \
    .add("unit_price", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("total_price", DoubleType()) \
    .add("order_timestamp", StringType())

logger.info(f"📡 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"📬 Subscribing to topic: {KAFKA_TOPIC}")
logger.info(f"💾 Writing to MySQL: {mysql_url}/{MYSQL_TABLE}")

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON messages
json_df = df.selectExpr("CAST(value AS STRING) as json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")


def write_to_mysql(batch_df, batch_id):
    """Write batch of orders to MySQL with error handling."""
    try:
        if batch_df.count() > 0:
            logger.info(f"🔥 Spark received batch {batch_id} with {batch_df.count()} records, writing to MySQL...")
            batch_df.show(truncate=False)
            
            batch_df.write \
                .format("jdbc") \
                .option("url", mysql_url) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", MYSQL_TABLE) \
                .option("user", MYSQL_USER) \
                .option("password", MYSQL_PASSWORD) \
                .mode("append") \
                .save()
            
            logger.info(f"✅ Successfully wrote batch {batch_id} to MySQL")
        else:
            logger.info(f"⏭️  Batch {batch_id} is empty, skipping...")
    except Exception as e:
        logger.error(f"❌ Error writing batch {batch_id} to MySQL: {e}", exc_info=True)
        # Continue processing despite errors


logger.info("🔄 Starting streaming query...")
query = parsed_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .start()

logger.info("✅ Stream started successfully. Waiting for messages...")
logger.info("Press Ctrl+C to stop\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("🛑 Stopping stream...")
    query.stop()
    logger.info("✅ Stream stopped successfully")
