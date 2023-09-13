import argparse
import json
import re
import time
from datetime import datetime
from threading import Thread

import websocket

from kafka import KafkaProducer

# Parse arguments
parser = argparse.ArgumentParser(
    prog="stock_producer", description="Produce data from Finnhub and publish to Kafka"
)

parser.add_argument(
    "--symbol",
    metavar="symbol",
    type=str,
    help="Symbol of asset to subscribe to",
    required=True
)

args = parser.parse_args()

# Refactoring string so that it is in the format that Kafka expects
asset_symbol = args.symbol
asset_symbol = re.sub(r'[^a-zA-Z0-9._-]', '_', asset_symbol)

# Kafka settings
KAFKA_BROKER_URL = "localhost:9092"
KAFKA_TOPIC_NAME = f"SPARK-{asset_symbol}"

# Finnhub settings
FINNHUB_API_KEY = "cjv0gd9r01qlmkvcsc6gcjv0gd9r01qlmkvcsc70"

# Kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL,
                            value_serializer=lambda x: json.dumps(x).encode("utf-8"))

def send_message_to_kafka(message):
    parsed_message = json.loads(message)
    if not parsed_message["data"]:
        return
    
    for entry in parsed_message["data"]:
        entry["t"] = datetime.fromtimestamp(entry["t"] / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")

        producer.send(KAFKA_TOPIC_NAME, value=entry)
        print("Sent Data")
    
    producer.flush()
    time.sleep(1)


def on_message(ws, message):
    Thread(target=send_message_to_kafka, args=(message,)).start()

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    message = {
        "type": "subscribe",
        "symbol": args.symbol
    }
    ws.send(json.dumps(message))

if __name__ == "__main__":
    # websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()