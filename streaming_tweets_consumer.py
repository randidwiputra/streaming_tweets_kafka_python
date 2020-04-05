from kafka import KafkaConsumer


kafka_consumer = KafkaConsumer('covid19indonesia', bootstrap_servers='localhost:9092')

for message in kafka_consumer:
    print(message)