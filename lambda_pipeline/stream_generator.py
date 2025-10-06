"""
Streaming data generator for the data pipeline.
"""
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
import os

def get_kafka_producer(bootstrap_servers='localhost:9092'):
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_stream_data():
    """Generate a single streaming data record."""
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'sensor_id': f'sensor_{random.randint(1, 10)}',
        'value': round(random.uniform(0, 100), 2),
        'status': random.choice(['OK', 'WARNING', 'ERROR'])
    }

def stream_data(producer, topic='stream_events', delay=1):
    """Continuously generate and send streaming data."""
    print(f"Starting to stream data to topic '{topic}'...")
    try:
        while True:
            data = generate_stream_data()
            producer.send(topic, value=data)
            print(f"Sent: {data}")
            time.sleep(delay)
    except KeyboardInterrupt:
        print("\nStopping data stream...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    kafka_server = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    producer = get_kafka_producer(kafka_server)
    stream_data(producer)
