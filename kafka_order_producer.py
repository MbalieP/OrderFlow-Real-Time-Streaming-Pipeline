"""
Kafka Order Producer
Generates simulated e-commerce order data and streams to Kafka

Features:
- Error handling and graceful shutdown
- Logging instead of print statements
- Configurable via environment variables
- Message delivery callbacks
- Automatic retry on connection failures
"""

import json
import logging
import os
import signal
import sys
import time
import random
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kafka_producer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration from environment variables with defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'orders')
PRODUCTION_INTERVAL = float(os.getenv('PRODUCTION_INTERVAL', '2'))  # seconds
STARTING_ORDER_ID = int(os.getenv('STARTING_ORDER_ID', '1'))

# Product catalog configuration
PRODUCT_CATEGORIES = [
    "Home & Garden",
    "Sports & Outdoors",
    "Food & Beverages",
    "Beauty & Health",
    "Electronics",
    "Fashion"
]

PRODUCT_NAMES = [
    "Garden Tools Set",
    "Yoga Mat",
    "Organic Coffee",
    "Skincare Bundle",
    "Wireless Headphones",
    "Designer Jeans"
]

# Global variables for graceful shutdown
producer = None
running = True


def generate_order(order_number: int) -> Dict[str, Any]:
    """
    Generate a simulated order payload.
    
    Args:
        order_number: Sequential order number
        
    Returns:
        Dictionary containing order data
    """
    unit_price = round(random.uniform(50.00, 999.99), 2)
    quantity = random.randint(1, 5)
    total_price = round(unit_price * quantity, 2)
    
    return {
        "customer_id": random.randint(50000, 99999),
        "order_id": f"ORDER-{order_number:06d}",
        "product_name": random.choice(PRODUCT_NAMES),
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "unit_price": unit_price,
        "quantity": quantity,
        "total_price": total_price,
        "order_timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
    }


def on_send_success(record_metadata):
    """Callback for successful message delivery."""
    logger.info(
        f"✅ Message sent successfully - "
        f"Topic: {record_metadata.topic}, "
        f"Partition: {record_metadata.partition}, "
        f"Offset: {record_metadata.offset}"
    )


def on_send_error(exception: KafkaError):
    """Callback for failed message delivery."""
    logger.error(f"❌ Failed to send message: {exception}", exc_info=True)


def create_producer() -> KafkaProducer:
    """
    Create and configure Kafka producer with retry logic.
    
    Returns:
        Configured KafkaProducer instance
    """
    max_retries = 5
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Producer configuration improvements
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,  # Retry failed sends
                max_in_flight_requests_per_connection=1,  # Ensure ordering
                enable_idempotence=True,  # Prevent duplicate messages
                compression_type='gzip'  # Compress messages
            )
            logger.info("✅ Successfully connected to Kafka")
            return producer
        except Exception as e:
            logger.warning(f"⚠️  Connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ Failed to connect to Kafka after all retries")
                raise


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global running
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    running = False


def main():
    """Main producer loop."""
    global producer, running
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Create producer
        producer = create_producer()
        
        current_order_number = STARTING_ORDER_ID
        logger.info(f"🚀 Starting order production. Topic: {KAFKA_TOPIC}, Interval: {PRODUCTION_INTERVAL}s")
        
        # Main production loop
        while running:
            try:
                # Generate order
                order_payload = generate_order(current_order_number)
                
                # Send to Kafka with callbacks
                future = producer.send(
                    KAFKA_TOPIC,
                    order_payload
                )
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)
                
                logger.info(f"📦 Published order #{current_order_number}: {order_payload['order_id']}")
                
                current_order_number += 1
                time.sleep(PRODUCTION_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received")
                running = False
                break
            except Exception as e:
                logger.error(f"Error in production loop: {e}", exc_info=True)
                time.sleep(PRODUCTION_INTERVAL)  # Continue despite errors
                
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup
        if producer:
            logger.info("Closing Kafka producer connection...")
            producer.flush(timeout=10)  # Wait for pending messages
            producer.close(timeout=10)
            logger.info("✅ Producer closed successfully")


if __name__ == "__main__":
    main()





