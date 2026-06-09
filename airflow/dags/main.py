import sys
import logging

sys.path.insert(0, '/opt/airflow')

from pyspark.sql import SparkSession
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ─── Config ─────────────────────────────────────────────────────────────────
# "csv"    → lần đầu chạy, đọc từ file local
# "bronze" → các lần sau, đọc từ HDFS Bronze
EXTRACT_SOURCE = "csv"

# ─── Spark Session ──────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SalesETL") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    logging.info("Pipeline started")
    print("=== Pipeline Started ===")

    # 1. Extract
    df = extract_data(spark, source=EXTRACT_SOURCE)
    df.show(5)

    # 2. Transform — trả về dict {"bronze", "silver", "gold"}
    results = transform_data(df)

    # Show kết quả Gold layer
    print("\n=== Gold Layer Preview ===")
    results["gold"].show(10)

    # 3. Load — ghi cả 3 tầng xuống HDFS
    load_data(
        results["bronze"],
        results["silver"],
        results["gold"]
    )

    logging.info("Pipeline completed successfully")
    print("=== Pipeline Completed ===")

except Exception as e:
    logging.error(f"Pipeline failed: {str(e)}")
    print(f"[ERROR] Pipeline failed: {str(e)}")
    raise

finally:
    spark.stop()