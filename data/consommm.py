from kafka import KafkaConsumer

# Kafka Consumer
consumer = KafkaConsumer(
    'Heart_Failure_Project',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Démarre la consommation depuis le début du topic
    group_id='my_consumer_group'  # Groupe de consommateurs pour le suivi
)

# Consommez les messages du topic
for message in consumer:
    print(f'Received message: {message.value.decode("utf-8")}')
