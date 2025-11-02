from kafka import KafkaProducer
import pandas as pd
import json
import time
import os

# Configuración de conexión
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "accidentes_topic"
CSV_FILE = "/home/vboxuser/kafka/NUMERODESINIESTROS.csv"

# Inicializar productor
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks=1
)

print("DEBUG: KAFKA_BOOTSTRAP =", KAFKA_BOOTSTRAP)
print("DEBUG: Archivo existe?", os.path.exists(CSV_FILE))

# Leer dataset
df = pd.read_csv(CSV_FILE, sep=',', encoding='latin-1', engine='python', low_memory=False)
print("DEBUG: Filas leídas del CSV:", len(df))

# Enviar fila a fila como JSON
for idx, row in df.iterrows():
    record = row.to_dict()
    record["ingest_ts"] = time.strftime("%Y-%m-%d %H:%M:%S")  # timestamp de envío
    producer.send(TOPIC, value=record)
    if idx % 100 == 0:
        print(f"Enviado registro {idx+1}/{len(df)}")
    time.sleep(0.05)  # simular tiempo real

producer.flush()
print("Envío completo.")
