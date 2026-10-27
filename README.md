
# Fase 3 - Big Data UNAD

*Estudiante:* Arizaraunad
*Curso:* Big Data

### Descripción
Proyecto de procesamiento en tiempo real y batch para sensores IoT usando Kafka y Spark.

### Estructura
- Kafka_Producer.py: Simula 5 sensores enviando temperatura y humedad al topic sensor_data cada 2 segundos.
- Spark_Streaming_cosumer.py: Consumidor en streaming con PySpark que lee desde Kafka.
- Analisis_batch.py: Análisis batch con pandas, promedio por sensor.

### Tecnologías
Python, Apache Kafka, Apache Spark Structured Streaming, Pandas.

### Cómo ejecutar
1. Iniciar Kafka: bin/kafka-server-start.sh
2. Crear topic: bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092
3. Ejecutar productor: python Kafka_Producer.py
4. Ejecutar consumidor Spark: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 Spark_Streaming_cosumer.py
   
