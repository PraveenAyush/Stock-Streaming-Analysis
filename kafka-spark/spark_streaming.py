import argparse
import json
import re

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, window, sum as _sum
from pyspark.sql.types import FloatType, StringType, StructField, StructType, TimestampType


# Kafka settings
KAFKA_BROKER_URL = "localhost:9092"

# Parse arguments
parser = argparse.ArgumentParser(
    prog="spark_streaming", description="Consume data from Kafka, aggregate and publish to Kafka"
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

# Spark Config
conf = SparkConf().setAppName("StockData") \
                  .set("spark.sql.shuffle.partitions", "2") \
                  .set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
                  .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
                  .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")

# Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Define Kafka Source and Configuration
kafka_params = {
    "kafka.bootstrap.servers": KAFKA_BROKER_URL,
    "subscribe": f"SPARK-{asset_symbol}",
    "startingOffsets": "earliest",
    "value_deserializer": lambda x: json.loads(x.decode('utf-8'))
}

# Define Schema
schema = StructType([
    StructField("p", FloatType(), True),
    StructField("s", StringType(), True),
    StructField("t", TimestampType(), True),
    StructField("v", FloatType(), True),
    StructField("c", StringType())
])

# Read from Kafka
df = spark.readStream.format("kafka").options(**kafka_params).load()

kafka_df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)") \
                .select(from_json(col("value"), schema).alias("data"))

# Aggregate
windowed_stream = kafka_df.groupBy(window(col("data.t"), "10 seconds", "10 seconds")) \
                            .agg(
                                avg(col("data.p")).alias("avg_price"),
                                _sum(col("data.v")).alias("total_volume")
                            ) 


# Write to console
console_write = ( windowed_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()
)

# Publish to Kafka
query = windowed_stream.selectExpr("to_json(struct(*)) AS value") \
                        .writeStream \
                        .outputMode("update") \
                        .trigger(processingTime="10 seconds") \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
                        .option("topic", asset_symbol) \
                        .start()

query.awaitTermination()
console_write.awaitTermination()
