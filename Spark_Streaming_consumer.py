# Spark_Streaming_consumer.py - Fase 3 UNAD - Procesamiento en Tiempo Real
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, max, min
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

print("=== INICIANDO SPARK STREAMING + KAFKA - FASE 3 ===")

# 1. Crear sesión Spark
spark = SparkSession.builder \
    .appName("IoT_Realtime_LaGuajira") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir esquema del JSON que envía el productor
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", StringType())

# 3. Configurar topic en Kafka - Leer en tiempo real
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

print("Conectado al topic: sensor_data")

# 4. Parsear JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 5. REALIZAR PROCESAMIENTO EN TIEMPO REAL (lo que pide la guía)

# a) Contar eventos por sensor en ventana de tiempo
from pyspark.sql.functions import window

df_with_time = df_parsed.withColumn("event_time", col("timestamp").cast(TimestampType()))

# Conteo en tiempo real
conteo_stream = df_with_time \
    .groupBy("sensor_id") \
    .agg(count("*").alias("total_eventos"),
         avg("temperature").alias("temp_promedio"),
         max("temperature").alias("temp_max"),
         min("temperature").alias("temp_min"),
         avg("humidity").alias("humedad_promedio"))

# b) Estadísticas en consola (visualización tiempo real)
query_consola = conteo_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# c) Visualización adicional - Guardar resultados en memoria para dashboard
query_memoria = df_with_time.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("vista_tiempo_real") \
    .start()

print("Procesamiento en tiempo real iniciado...")
print("Esperando datos del productor Kafka_Producer.py")
print("Estadísticas: conteo de eventos y promedio por sensor cada 5 segundos")

query_consola.awaitTermination()
