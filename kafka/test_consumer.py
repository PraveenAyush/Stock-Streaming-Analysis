import json

from kafka import KafkaConsumer

# Kafka settings
KAFKA_BROKER_URL = "localhost:9092"
KAFKA_TOPIC_NAME = "BINANCE-BTCUSDT"

# Kafka consumer
consumer = KafkaConsumer(KAFKA_TOPIC_NAME, bootstrap_servers=KAFKA_BROKER_URL,
                            value_deserializer=lambda x: json.loads(x.decode("utf-8")))
for message in consumer:
    print(message.value)
