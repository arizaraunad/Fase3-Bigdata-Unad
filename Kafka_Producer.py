from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

sensors = [f"sensor_{i}" for i in range(5)]
while True:
    data = {
        "sensor_id": random.choice(sensors),
        "temperature": round(random.uniform(20, 35), 2),
        "humidity": round(random.uniform(50, 80), 2),
        "timestamp": datetime.now().isoformat()
    }
    print(f"Enviando: {data}")
    producer.send('sensor_data', data)
    time.sleep(2)
  
