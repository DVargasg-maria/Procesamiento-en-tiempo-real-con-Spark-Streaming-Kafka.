from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, desc
from pyspark.sql.types import StructType, StringType

# --- Sesión Spark ---
spark = SparkSession.builder.appName("StreamingAccidentesBogota").getOrCreate()

# --- Esquema esperado del JSON ---
schema = StructType() \
    .add("CODIGO_ACC", StringType()) \
    .add("FECHA_OCUR", StringType()) \
    .add("HORA_OCURR", StringType()) \
    .add("LOCALIDAD", StringType()) \
    .add("CLASE_ACC", StringType()) \
    .add("GRAVEDAD", StringType()) \
    .add("LATITUD", StringType()) \
    .add("LONGITUD", StringType()) \
    .add("ingest_ts", StringType())

# --- Lectura desde Kafka ---
kafka_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "accidentes_topic")
            .option("startingOffsets", "latest")
            .load())

# --- Parsear JSON ---
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# --- Convertir timestamp de ingreso ---
parsed = parsed.withColumn("ingest_ts_ts", to_timestamp(col("ingest_ts")))

# --- Procesamiento: conteo por LOCALIDAD en ventana de 1 minuto ---
agg_window = (parsed
              .withWatermark("ingest_ts_ts", "2 minutes")
              .groupBy(window(col("ingest_ts_ts"), "1 minute", "30 seconds"), col("LOCALIDAD"))
              .count()
              .orderBy(desc("count")))

# --- Salida 1: consola ---
query_console = (agg_window.writeStream
                 .outputMode("complete")
                 .format("console")
                 .option("truncate", False)
                 .start())

# --- Salida 2: Parquet ---
query_parquet = (agg_window.writeStream
                 .outputMode("append")
                 .format("parquet")
                 .option("path", "/home/vboxuser/kafka/stream_output_parquet")
                 .option("checkpointLocation", "/home/vboxuser/kafka/stream_checkpoint")
                 .start())

# --- Esperar terminación ---
spark.streams.awaitAnyTermination()
