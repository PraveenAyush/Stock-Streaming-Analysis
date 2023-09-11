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

insert_query = "INSERT INTO stock_data (price, symbol, data_datetime, volume) VALUES (%s, %s, %s, %s)"

values = []
row_count = 0
batch_count = 0

# Kafka consumer
consumer = KafkaConsumer(KAFKA_TOPIC_NAME, bootstrap_servers=KAFKA_BROKER_URL)

for message in consumer:
    data = json.loads(message.value.decode('utf-8'))

    values.append((data['p'], data['s'], data['t'], data['v']))

    row_count += 1
    batch_count += 1

    if batch_count == 10:
        cur.executemany(insert_query, values)
        conn.commit()
        values = []
        batch_count = 0

        print(f"Inserted {row_count} rows into MySQL")