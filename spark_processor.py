import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', '127.0.0.1:9092')

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Initialize Spark
spark = SparkSession.builder \
    .appName("SalesAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2") \
    .getOrCreate()

schema = StructType([
    StructField("category", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType())
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "sales_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 2-minute window and watermark reduce the number of small writes to the DB
processed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "2 minutes"),
        col("category")
    ).agg(sum("amount").alias("amount"))


def write_to_postgres(df, epoch_id):
    final_df = df.select(
        col("category"),
        col("amount"),
        col("window.end").alias("timestamp")
    )

    final_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "sales_historical") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .save()


# 1-minute processing trigger batches writes instead of writing continuously
query = processed_stream.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .start()

query.awaitTermination()