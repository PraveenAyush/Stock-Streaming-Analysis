import argparse
import mysql.connector
import json

from kafka import KafkaConsumer

# Parse arguments
parser = argparse.ArgumentParser(
    prog="db_consumer", description="Consume data from Kafka and insert into MySQL"
)

parser.add_argument(
    "--topic",
    metavar="topic",
    type=str,
    help="Kafka topic to consume from (Refactored symbol))",
    required=True
)

args = parser.parse_args()

# Function to create MySQL table if it doesn't exist
def create_table_if_not_exists(table_name) -> None:
    query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            price DECIMAL,
            volume FLOAT,
            price_datetime TIMESTAMP(3)
        )
    """
    cur.execute(query)
    conn.commit()
    

# Kafka settings
KAFKA_BROKER_URL = "localhost:9092"
KAFKA_TOPIC_NAME = args.topic

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

# Create table if it doesn't exist
create_table_if_not_exists(KAFKA_TOPIC_NAME)

for message in consumer:
    data = json.loads(message.value.decode('utf-8'))

    cur.execute(
        f"INSERT INTO {KAFKA_TOPIC_NAME} (price, volume, price_datetime) VALUES (%s, %s, %s)",
        [
            data['avg_price'],
            data['total_volume'],
            data['window']['end']
        ]
    )

    conn.commit()

    print(f"Inserted {data['window']['end']} into MySQL")

cur.close()
conn.close()

