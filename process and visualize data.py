from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from confluent_kafka import Consumer, KafkaError, Producer
import streamlit as st
import matplotlib.pyplot as plt
from datetime import datetime

# Start SparkSession
spark = SparkSession.builder \
    .appName("Real-Time Network Traffic Analysis") \
    .getOrCreate()

# Configure Kafka connection details
bootstrap_servers = '<server_name>'
sasl_username = '<username>'
sasl_password = '<password>'
kafka_topic = 'network-traffic'
processed_topic = 'processed-data'

# Define schema for the data
schema = StructType([
    StructField("source_ip", StringType(), True),
    StructField("destination_ip", StringType(), True),
    StructField("bytes_sent", IntegerType(), True),
    StructField("event_time", TimestampType(), True)
])
# Create Kafka consumer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': sasl_username,
    'sasl.password': sasl_password,
    'group.id': 'network-traffic-group',
    'auto.offset.reset': 'earliest'
}

# Create a Streamlit app
st.title("Real-Time Network Traffic Analysis")


# configure Kafka consumer
consumer = Consumer(conf)
producer = Producer(conf)

# Subscribe to the Kafka topic
consumer.subscribe([kafka_topic])

# Read data from Kafka and perform real-time visualization
def read_from_kafka():

    # Create a plot to visualize processed data
    fig, ax = plt.subplots()

    # Initialize an empty list to store processed data and event times
    processed_data = []
    event_times = []

    # Function to process incoming Kafka messages and update the plot
    def process_message(message):
        nonlocal processed_data, event_times
        if message is None:
            return
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                return
            else:
                print(f"Error: {message.error()}")
                return

        value = message.value().decode('utf-8')
        data = value.split(',')
        source_ip = data[0]
        destination_ip = data[1]
        bytes_sent = int(data[2])
        event_time = datetime.now()

        row = (source_ip, destination_ip, bytes_sent, event_time)
        df = spark.createDataFrame([row], schema=schema)

        # Perform window-based aggregations
        aggregated_df = df \
            .groupBy("source_ip") \
            .agg(sum("bytes_sent").alias("total_bytes_sent")) \
            .orderBy(desc("total_bytes_sent"))

        # Convert the aggregated DataFrame to JSON
        json_data = aggregated_df.select(to_json(struct("*")).alias("value")).first().value

        # Publish processed data to Kafka topic
        producer.produce(processed_topic, value=json_data.encode('utf-8'))

        # Wait for the message to be delivered to Kafka
        producer.flush()

        # Process the Kafka message and update the plot
        processed_data.append(bytes_sent)
        event_times.append(event_time)
        ax.clear()
        ax.plot(event_times, processed_data)
        ax.set_xlabel("Event Time")
        ax.set_ylabel("Processed Data")
        st.pyplot(fig)

    # Continuously read messages from Kafka topic
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {message.error()}")
                break

        process_message(message)

# Run the Streamlit app
read_from_kafka()