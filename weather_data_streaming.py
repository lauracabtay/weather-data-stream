import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

spark = SparkSession.builder.appName("KafkaSparkIntegration").getOrCreate()

schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("weather", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("pressure", IntegerType(), True),
])

# Create a Kafka consumer using Spark Structured Streaming
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .load()

# Deserialize Kafka message value (assuming it's JSON)
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("weather_data", from_json("value", schema))

df = df.select(
    col("key"),
    col("weather_data.timestamp").alias("forecast-timestamp"),
    col("weather_data.country").alias("country"),
    col("weather_data.city").alias("city"),
    col("weather_data.weather").alias("weather"),
    col("weather_data.temperature").alias("temperature"),
    col("weather_data.wind_speed").alias("wind_speed"),
    col("weather_data.humidity").alias("humidity"),
    col("weather_data.pressure").alias("pressure")
)

result = df.groupBy("country").agg(
    round(avg("temperature"), 2).alias("avg_temperature"),
    round(avg("wind_speed"), 2).alias("avg_wind_speed"),
    round(avg("humidity"), 2).alias("avg_humidity"),
    round(avg("pressure"), 2).alias("avg_pressure")
)

query = result.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="60 seconds") \
    .start()

query.awaitTermination()
