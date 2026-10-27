import pandas as pd
# Análisis batch - reemplaza con tu CSV si lo tienes
print("Análisis histórico de sensores IoT")
# Ejemplo de agregación
data = {"sensor_id": ["s1", "s2"], "temperature": [25.5, 28.1]}
df = pd.DataFrame(data)
print(df.groupby("sensor_id").mean())
