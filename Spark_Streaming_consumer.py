from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder.appName("IoTStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType().add("sensor_id", StringType()).add("temperature", DoubleType()).add("humidity", DoubleType()).add("timestamp", StringType())

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "sensor_data").load()
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

query = json_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
