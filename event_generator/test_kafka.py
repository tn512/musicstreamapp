import json
import logging
import time
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Set to DEBUG for more detailed logs
logger = logging.getLogger(__name__)

def test_kafka_connection(broker):
    """Test if we can connect to Kafka broker"""
    try:
        # Create a Kafka admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=broker,
            client_id='test-admin',
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='SCRAM-SHA-256',
            sasl_plain_username='client',
            sasl_plain_password='lbjKWoE6nr'
        )
        
        # List topics to test connection
        topics = admin_client.list_topics()
        logging.debug(f"Successfully connected to Kafka. Topics: {topics}")
        admin_client.close()
        return True
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
        return False

def test_send_receive(broker):
    """Test sending and receiving a message"""
    try:
        # Create a producer
        producer = KafkaProducer(
            bootstrap_servers=broker,
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='SCRAM-SHA-256',
            sasl_plain_username='client',
            sasl_plain_password='lbjKWoE6nr'
        )
        
        # Send a test message
        test_message = json.dumps({'test': 'message'}).encode('utf-8')
        future = producer.send('test_topic', test_message)
        result = future.get(timeout=60)
        logging.debug(f"Message sent to partition {result.partition} at offset {result.offset}")
        producer.close()
        
        # Create a consumer
        consumer = KafkaConsumer(
            'test_topic',
            bootstrap_servers=broker,
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='SCRAM-SHA-256',
            sasl_plain_username='client',
            sasl_plain_password='lbjKWoE6nr',
            group_id='test-group',
            auto_offset_reset='earliest'
        )
        
        # Try to get the message
        logging.debug("Waiting for messages...")
        messages = consumer.poll(timeout_ms=5000)
        consumer.close()
        
        if messages:
            for tp, msgs in messages.items():
                for msg in msgs:
                    logging.debug(f"Received message: {msg.value.decode('utf-8')}")
            return True
        else:
            logging.error("No messages received")
            return False
            
    except Exception as e:
        logging.error(f"Error in test_send_receive: {e}")
        return False

def main():
    broker = "localhost:9092"
    print(f"Testing Kafka connection to {broker}")
    
    if test_kafka_connection(broker):
        test_send_receive(broker)
    else:
        print("❌ Failed to connect to Kafka")

if __name__ == "__main__":
    main() 