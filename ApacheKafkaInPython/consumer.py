import csv
import yaml
from datetime import datetime

from kafka import KafkaConsumer

config_file = 'config.yaml'
with open(config_file, 'r') as file:
    config = yaml.safe_load(file)

TOPIC = config['kafka']['topic']
BROKERS = config['kafka']['brokers']
GROUP = config['kafka']['group']
SASL_USERNAME = config['kafka']['sasl_username']
SASL_PASSWORD = config['kafka']['sasl_password']
key_pattern = config['key_pattern']


# Set up the Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKERS,
    group_id=GROUP,  # Consumer group ID
    auto_offset_reset='earliest',  # Start reading at the earliest available message
    security_protocol='SASL_PLAINTEXT',  # Use SASL for authentication
    sasl_mechanism='PLAIN',  # SASL mechanism
    sasl_plain_username=SASL_USERNAME,
    sasl_plain_password=SASL_PASSWORD,
    value_deserializer=lambda m: m.decode('utf-8'),  # Decode message value
    key_deserializer=lambda k: k.decode('utf-8') if k else None  # Decode message key
)


def filter_key(key):
    return key and any(pattern in key for pattern in key_pattern)


# Generate a timestamp for the filename
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
filename = f'kafka_messages_{timestamp}.csv'


def consume_messages(writer):
    # Consume and filter messages
    try:
        for message in consumer:
            if filter_key(key=message.key):
                writer.writerow([
                    message.offset,
                    message.partition,
                    message.timestamp,
                    message.key,
                    message.value
                ])
                print(f"Message written: Offset: {message.offset}, Key: {message.key}, Message: {message.value}\n")
    finally:
        consumer.close()


def main():
    # Open the CSV file for writing
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter='|')
        # Write the header row
        writer.writerow(['Offset', 'Partition', 'Timestamp', 'Key', 'Message'])
        consume_messages(writer)


if __name__ == '__main__':
    main()
