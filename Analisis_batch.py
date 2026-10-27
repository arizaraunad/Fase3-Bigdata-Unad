# Analisis_batch.py - Fase 3 UNAD - Cumple punto 3: Procesamiento Batch
import pandas as pd
import matplotlib.pyplot as plt

print("=== FASE 3: PROCESAMIENTO BATCH - IoT La Guajira ===")

# 1. CARGAR CONJUNTO DE DATOS DESDE FUENTE ORIGINAL
# Simulamos carga de datos históricos (en caso real vendría de HDFS / CSV / Kaggle)
try:
    df = pd.read_csv("sensor_data_historico.csv")
    print("Dataset cargado desde CSV")
except:
    # Si no existe, creamos dataset de ejemplo (500 registros)
    print("Creando dataset histórico simulado...")
    import random
    from datetime import datetime, timedelta
    data = []
    base = datetime.now()
    for i in range(500):
        data.append({
            "sensor_id": f"sensor_{random.randint(0,4)}",
            "temperature": round(random.uniform(20, 38), 2),
            "humidity": round(random.uniform(45, 85), 2),
            "timestamp": (base - timedelta(minutes=i*2)).isoformat()
        })
    df = pd.DataFrame(data)
    df.to_csv("sensor_data_historico.csv", index=False)

print(f"Registros cargados: {len(df)}")
print(df.head())

# 2. OPERACIONES DE LIMPIEZA, TRANSFORMACIÓN Y EDA CON DATAFRAMES (y RDDs)
# Limpieza: eliminar nulos y duplicados
df_clean = df.dropna().drop_duplicates()
print(f"Después de limpieza: {len(df_clean)} registros")

# Transformación: convertir timestamp y crear columnas
df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])
df_clean['hora'] = df_clean['timestamp'].dt.hour

# Análisis exploratorio (EDA) usando DataFrames
print("\n--- EDA con DataFrames ---")
print(df_clean.describe())
print("\nPromedio por sensor:")
print(df_clean.groupby("sensor_id").agg({"temperature":"mean", "humidity":"mean"}))

# EDA con RDDs (concepto del punto 1 - para cumplir guía)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BatchEDA").getOrCreate()
spark_df = spark.createDataFrame(df_clean)
rdd = spark_df.rdd
print(f"\n--- EDA con RDDs ---")
print(f"Total RDD count: {rdd.count()}")
print(f"RDD filtrado (temp > 30): {rdd.filter(lambda x: x['temperature'] > 30).count()} registros críticos")
print(f"RDD map ejemplo - temperaturas: {rdd.map(lambda x: x['temperature']).take(3)}")

# 3. VISUALIZAR RESULTADOS DEL PROCESAMIENTO BATCH
plt.figure(figsize=(10,4))
plt.subplot(1,2,1)
df_clean.groupby("sensor_id")["temperature"].mean().plot(kind='bar', color='orange')
plt.title("Temperatura promedio por sensor")
plt.ylabel("°C")

plt.subplot(1,2,2)
plt.hist(df_clean["temperature"], bins=20, color='skyblue')
plt.title("Distribución de Temperatura")
plt.xlabel("Temperatura")

plt.tight_layout()
plt.savefig("resultados_batch.png")
print("\nGráfico guardado como resultados_batch.png")
print("Análisis batch completado con éxito - Listo para informe Fase 3")
