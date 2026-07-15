from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg
from pyspark.sql.types import StructType, IntegerType, StructField

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaPairWiseAverage") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema (IMPORTANT: includes group id)
schema = StructType([
    StructField("group", IntegerType(), True),
    StructField("number", IntegerType(), True)
])

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "numbers") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value (bytes → string)
json_df = df.selectExpr("CAST(value AS STRING) as json")

# Parse JSON
parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
).select(
    col("data.group").alias("group"),
    col("data.number").alias("number")
)

# Compute average per pair (group)
avg_df = parsed_df.groupBy("group").agg(
    avg("number").alias("pair_average")
)

# Write output to console
query = avg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()