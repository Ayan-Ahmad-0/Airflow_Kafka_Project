from kafka import KafkaProducer
import json
import time
import random

KAFKA_BROKER = "kafka:9092"   # Match docker-compose service name
KAFKA_TOPIC = "taxi_data_new"

def generate_message():
    return {
        "taxi_id": random.randint(1000, 9999),
        "timestamp": time.time(),
        "pickup_location": random.choice(["Manhattan", "Brooklyn", "Queens"]),
        "dropoff_location": random.choice(["Manhattan", "Brooklyn", "Queens"]),
        "passenger_count": random.randint(1, 4)
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    message_count = 10  
    for i in range(message_count):
        message = generate_message()
        producer.send(KAFKA_TOPIC, message)
        print(f"Sent message {i+1}: {message}")
        time.sleep(1)  

    producer.flush()
    producer.close()
    print("Finished sending messages")

if __name__ == "__main__":
    main()


