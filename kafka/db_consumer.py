import mysql.connector
import json

from kafka import KafkaConsumer

# Kafka settings
KAFKA_BROKER_URL = "localhost:9092"
KAFKA_TOPIC_NAME = "BINANCE-BTCUSDT"

# MySQL settings
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DATABASE = "stock"
MYSQL_PORT = "3306"

# MySQL connection
conn = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE,
                                port=MYSQL_PORT)
cur = conn.cursor()

# Kafka consumer
consumer = KafkaConsumer(KAFKA_TOPIC_NAME, bootstrap_servers=KAFKA_BROKER_URL)

for message in consumer:
    data = json.loads(message.value.decode('utf-8'))

    cur.execute(
        "INSERT INTO stock_data (price, volume, price_datetime) VALUES (%s, %s, %s)",
        [
            data['avg_price'],
            data['total_volume'],
            data['window']['end']
        ]
    )

    conn.commit()

    print(f"Inserted {data['window']['end']} into MySQL")

