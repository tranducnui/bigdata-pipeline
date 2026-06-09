from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, lower, trim,
    round as spark_round, current_timestamp,
    to_timestamp, regexp_replace
)


def transform_bronze(df: DataFrame) -> DataFrame:
    """
    Bronze layer — KHÔNG transform, chỉ thêm metadata.
    Raw data phải được giữ nguyên 100% như nguồn.
    """
    return df.withColumn("ingested_at", current_timestamp())


def transform_silver(df: DataFrame) -> DataFrame:
    """
    Silver layer — clean, validate, standardize.
    Không aggregate, chỉ làm sạch từng record.
    """
    return df \
        .filter(col("customer_name").isNotNull()) \
        .filter(col("amount").isNotNull()) \
        .filter(col("amount") > 0) \
        .withColumn("customer_name", lower(trim(col("customer_name")))) \
        .withColumn("amount", spark_round(col("amount"), 2)) \
        .withColumn("processed_at", current_timestamp()) \
        .dropDuplicates(["customer_id", "amount"])


def transform_gold(df: DataFrame) -> DataFrame:
    """
    Gold layer — aggregate, business logic.
    Đây là tầng Power BI / Trino sẽ query trực tiếp.
    """
    return df \
        .groupBy("customer_id", "customer_name") \
        .agg(
            spark_sum("amount").alias("total_spent")
        ) \
        .withColumn("total_spent", spark_round(col("total_spent"), 2)) \
        .withColumn("updated_at", current_timestamp()) \
        .orderBy(col("total_spent").desc())


def transform_data(df: DataFrame) -> dict:
    """
    Main transform function — chạy toàn bộ Medallion pipeline.
    Trả về dict chứa 3 DataFrames tương ứng 3 tầng.
    """
    print("=== Starting Medallion Transform ===")

    df_bronze = transform_bronze(df)
    print(f"[Bronze] {df_bronze.count()} rows — raw data")

    df_silver = transform_silver(df_bronze)
    print(f"[Silver] {df_silver.count()} rows — after cleaning")

    df_gold = transform_gold(df_silver)
    print(f"[Gold]   {df_gold.count()} rows — after aggregation")

    print("=== Medallion Transform Complete ===")

    return {
        "bronze": df_bronze,
        "silver": df_silver,
        "gold":   df_gold
    }