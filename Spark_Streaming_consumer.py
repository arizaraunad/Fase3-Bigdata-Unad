
# Spark_Streaming_consumer.py - VERSION QUE SI MUESTRA PROMEDIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("IoT_LaGuajira") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") # Para que no llene la pantalla de INFO

schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")).select("data.*")

# ESTO ES LO QUE TE FALTABA: el promedio
stats = json_df.groupBy("sensor_id").agg(
    count("*").alias("total_eventos"),
    avg("temperature").alias("promedio_temperatura"),
    avg("humidity").alias("promedio_humedad")
)

# Este trigger es clave para que aparezca cada 5 seg
query = stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()
