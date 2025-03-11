import json
import time
from kafka import KafkaProducer

# Function to serialize data into JSON format before sending to Kafka
def json_serializer(data):
    return json.dumps(data).encode('utf-8')  # Convert Python dict to UTF-8 encoded JSON string

# Define the Kafka broker address
server = 'localhost:9092'  # Kafka broker is running locally on port 9092

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[server],  # Specify Kafka broker address
    value_serializer=json_serializer  # Use the defined JSON serializer
)

# Start the timer to measure execution time
t0 = time.time()

# Define Kafka topic name
topic_name = 'test-topic'  # Messages will be sent to this Kafka topic

# Produce and send messages to Kafka
for i in range(10, 1000):  # Loop to generate messages with test data
    message = {
        'test_data': i,  # Example payload
        'event_timestamp': time.time() * 1000  # Capture current timestamp in milliseconds
    }

    producer.send(topic_name, value=message)  # Send message to Kafka
    print(f"Sent: {message}")  # Print confirmation
    time.sleep(0.05)  # Introduce a 50ms delay between messages (simulating real-time data)

# Ensure all messages are flushed (sent) before closing the producer
producer.flush()

# End the timer and print execution time
t1 = time.time()
print(f'Took {(t1 - t0):.2f} seconds')  # Print the total execution time
