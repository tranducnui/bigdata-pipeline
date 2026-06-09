from pyspark.sql import SparkSession, DataFrame

HDFS_BRONZE = "hdfs://namenode:9000/data/bronze/streaming/sales"

def extract_from_bronze(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(HDFS_BRONZE)
    print(f"[Extract] Read {df.count()} rows from Bronze: {HDFS_BRONZE}")
    return df

def extract_data(spark: SparkSession, source: str = "bronze") -> DataFrame:
    return extract_from_bronze(spark)