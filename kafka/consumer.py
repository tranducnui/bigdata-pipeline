from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BROKER        = 'kafka:29092'         # trong Docker dùng internal port
TOPIC               = 'sales_topic'
HDFS_BRONZE         = 'hdfs://namenode:9000/data/bronze/streaming/sales'
CHECKPOINT_LOCATION = 'hdfs://namenode:9000/checkpoint/streaming/sales'

# ─── Schema của message từ Kafka ─────────────────────────────────────────────
SCHEMA = StructType([
    StructField("transaction_id",   StringType(), True),
    StructField("customer_id",      StringType(), True),
    StructField("customer_name",    StringType(), True),
    StructField("bank",             StringType(), True),
    StructField("account",          StringType(), True),
    StructField("city",             StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("category",         StringType(), True),
    StructField("amount",           DoubleType(), True),
    StructField("currency",         StringType(), True),
    StructField("status",           StringType(), True),
    StructField("timestamp",        StringType(), True),
    StructField("device",           StringType(), True),
])

# ─── Spark Session ────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SalesStreamingConsumer") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─── Read từ Kafka ────────────────────────────────────────────────────────────
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# ─── Parse JSON message ───────────────────────────────────────────────────────
df_parsed = df_raw \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), SCHEMA)) \
    .select("data.*") \
    .withColumn("ingested_at", current_timestamp())

# ─── Write xuống HDFS Bronze (append-only) ───────────────────────────────────
query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", HDFS_BRONZE) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print(f"Consumer running — writing to {HDFS_BRONZE}")
print("Press Ctrl+C to stop\n")

query.awaitTermination()