import csv
from datetime import datetime

from kafka import KafkaConsumer

TOPIC = "hypertrail-dev-v2"
BROKERS = ["54.81.123.72:9093"]
GROUP = "freshservice-workflow-service"
SASL_USERNAME = "JFMDSFVTTY2BXQT2"
SASL_PASSWORD = "5F(Ug5eYRb2YcKZaaYp9V@uK(PMg^3oy7hYW6E4e^6#N@f3ZyCU6DSg4wjHSu$s!"

# Define the key pattern to filter on
key_pattern = ["workspace_create", "account_create", "workspace_destroy", "account_destroy"]

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
