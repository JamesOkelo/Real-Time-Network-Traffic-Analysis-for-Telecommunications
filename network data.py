


from kafka import KafkaProducer
import time
import random


# configure Kafka producer
bootstrap_servers = '<server_name>'
sasl_username = '<username>'
sasl_password = '<password>'
topic_name = 'network-traffic'

k_producer = KafkaProducer(
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': sasl_username,
    'sasl.password': sasl_password
)


# Generate and publish network traffic data to Kafka topic
while True:
    # Generate random network traffic data
    source_ip = '.'.join(str(random.randint(0, 255)) for _ in range(4))
    destination_ip = '.'.join(str(random.randint(0, 255)) for _ in range(4))
    bytes_sent = random.randint(1000, 100000)

    # Publish network traffic data to Kafka topic
    producer.send(topic_name, f"{source_ip},{destination_ip},{bytes_sent}".encode('utf-8'))

    # Wait for the message to be delivered to Kafka
    k_producer.flush()

    # Wait for 1 second before generating next network traffic data
    time.sleep(1)