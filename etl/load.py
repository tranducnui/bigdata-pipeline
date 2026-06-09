from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, col, round as spark_round
from datetime import datetime


# ─── HDFS paths (Medallion Architecture) ────────────────────────────────────
HDFS_BASE   = "hdfs://namenode:9000"
BRONZE_PATH = f"{HDFS_BASE}/data/bronze/batch/sales"
SILVER_PATH = f"{HDFS_BASE}/data/silver/customer_spending"
GOLD_PATH   = f"{HDFS_BASE}/data/gold/customer_spending"


def load_bronze(df: DataFrame):
    """
    Bronze layer — raw data, append-only, immutable.
    """
    partition_date = datetime.now().strftime("%Y-%m-%d")

    df.write \
      .mode("append") \
      .parquet(f"{BRONZE_PATH}/date={partition_date}/")

    print(f"[Bronze] Loaded → {BRONZE_PATH}/date={partition_date}/")


def load_silver(df: DataFrame):
    """
    Silver layer — cleaned, validated.
    Overwrite theo ngày để idempotent.
    """
    partition_date = datetime.now().strftime("%Y-%m-%d")

    df.write \
      .mode("overwrite") \
      .parquet(f"{SILVER_PATH}/date={partition_date}/")

    print(f"[Silver] Loaded → {SILVER_PATH}/date={partition_date}/")


def load_gold(df: DataFrame):
    """
    Gold layer — aggregated, business-ready.
    Overwrite toàn bộ vì đây là kết quả mới nhất.
    Hive/Trino/Power BI query từ đây.
    """
    df.write \
      .mode("overwrite") \
      .parquet(GOLD_PATH)

    print(f"[Gold] Loaded → {GOLD_PATH}")


def load_data(df_bronze: DataFrame, df_silver: DataFrame, df_gold: DataFrame):
    """
    Main load function — ghi cả 3 tầng xuống HDFS.
    """
    print("=== Starting Medallion Load ===")
    load_bronze(df_bronze)
    load_silver(df_silver)
    load_gold(df_gold)
    print("=== Medallion Load Complete ===")