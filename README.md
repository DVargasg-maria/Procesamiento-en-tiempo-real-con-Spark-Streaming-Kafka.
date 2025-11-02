# Procesamiento de datos de accidentes en tiempo real (Kafka + Spark Streaming)

Este proyecto implementa un flujo completo de **procesamiento de datos en tiempo real** usando **Apache Kafka** y **Apache Spark Structured Streaming**, con datos de accidentes de tránsito en Bogotá (dataset de Kaggle).

---

## Requisitos
- Ubuntu Server 22.04 o similar  
- Docker y Docker Compose  
- Apache Spark 3.4+ con soporte Kafka (`spark-sql-kafka-0-10_2.12`)  
- Python 3.8+ con entorno virtual (`venv-kafka`)

---

## 1. Levantar infraestructura Kafka
```bash
cd ~/kafka
docker-compose up -d
docker ps

---

Confirma que existan los contenedores kafka-kafka-1 y kafka-zookeeper-1.

---

## 2. Crear el topic
docker exec -it kafka-kafka-1 kafka-topics --create \
  --topic accidentes_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

## 3. Ejecutar el productor
source venv-kafka/bin/activate
python3 producer_accidentes.py

---

Envía los registros del dataset al topic Kafka simulando flujo en tiempo real.

---

## 4. Ejecutar el consumidor (Spark Streaming)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 streaming_consumer.py

---

Verás en consola microbatches con conteos por LOCALIDAD.

---

## 5. Resultados
-- Se guardan automáticamente en:
/home/vboxuser/kafka/stream_output_parquet
-- Se pueden explorar en:
df = spark.read.parquet("/home/vboxuser/kafka/stream_output_parquet")
df.show(10, truncate=False)


