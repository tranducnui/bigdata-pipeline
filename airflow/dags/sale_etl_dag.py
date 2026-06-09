from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import logging

sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─── HDFS paths ──────────────────────────────────────────────────────────────
HDFS_BASE       = "hdfs://namenode:9000"
HDFS_EXTRACTED  = f"{HDFS_BASE}/tmp/airflow/extracted"
HDFS_BRONZE_TMP = f"{HDFS_BASE}/tmp/airflow/bronze"
HDFS_SILVER_TMP = f"{HDFS_BASE}/tmp/airflow/silver"
HDFS_GOLD_TMP   = f"{HDFS_BASE}/tmp/airflow/gold"


def get_spark(app_name="SalesETL"):
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", HDFS_BASE) \
        .getOrCreate()


# ── Task 1: Extract ──────────────────────────────────────────────────────────
def extract(**context):
    from etl.extract import extract_data

    spark = get_spark("SalesETL_Extract")

    df = extract_data(spark, source="bronze")
    count = df.count()
    logging.info(f"Extracted {count} rows")

    df.write.mode("overwrite").parquet(HDFS_EXTRACTED)
    context['ti'].xcom_push(key='row_count', value=count)

    spark.stop()


# ── Task 2: Transform ────────────────────────────────────────────────────────
def transform(**context):
    from etl.transform import transform_data

    spark = get_spark("SalesETL_Transform")

    df = spark.read.parquet(HDFS_EXTRACTED)
    results = transform_data(df)

    results["bronze"].write.mode("overwrite").parquet(HDFS_BRONZE_TMP)
    results["silver"].write.mode("overwrite").parquet(HDFS_SILVER_TMP)
    results["gold"].write.mode("overwrite").parquet(HDFS_GOLD_TMP)

    logging.info("Transform completed — bronze/silver/gold saved to HDFS")
    spark.stop()


# ── Task 3: Load ─────────────────────────────────────────────────────────────
def load(**context):
    from etl.load import load_data

    spark = get_spark("SalesETL_Load")

    df_bronze = spark.read.parquet(HDFS_BRONZE_TMP)
    df_silver = spark.read.parquet(HDFS_SILVER_TMP)
    df_gold   = spark.read.parquet(HDFS_GOLD_TMP)

    load_data(df_bronze, df_silver, df_gold)

    logging.info("Load completed — data written to Bronze/Silver/Gold layers")
    spark.stop()


# ── Task 4: Create Hive Tables ───────────────────────────────────────────────
def create_hive_tables(**context):
    import subprocess

    ddl = """
    -- Bronze table
    CREATE EXTERNAL TABLE IF NOT EXISTS bronze.sales_streaming (
        transaction_id   STRING,
        customer_id      STRING,
        customer_name    STRING,
        bank             STRING,
        account          STRING,
        city             STRING,
        transaction_type STRING,
        category         STRING,
        amount           DOUBLE,
        currency         STRING,
        status           STRING,
        event_timestamp  STRING,
        device           STRING,
        ingested_at      TIMESTAMP
    )
    STORED AS PARQUET
    LOCATION 'hdfs://namenode:9000/data/bronze/streaming/sales/';

    -- Silver table
    CREATE EXTERNAL TABLE IF NOT EXISTS silver.customer_spending (
        transaction_id   STRING,
        customer_id      STRING,
        customer_name    STRING,
        bank             STRING,
        account          STRING,
        city             STRING,
        transaction_type STRING,
        category         STRING,
        amount           DOUBLE,
        currency         STRING,
        status           STRING,
        device           STRING,
        processed_at     TIMESTAMP
    )
    STORED AS PARQUET
    LOCATION 'hdfs://namenode:9000/data/silver/customer_spending/';

    -- Gold table
    CREATE EXTERNAL TABLE IF NOT EXISTS gold.customer_spending (
        customer_id   STRING,
        customer_name STRING,
        total_spent   DOUBLE,
        updated_at    TIMESTAMP
    )
    STORED AS PARQUET
    LOCATION 'hdfs://namenode:9000/data/gold/customer_spending/';
    """

    result = subprocess.run(
        ["beeline", "-u", "jdbc:hive2://hiveserver2:10000", "-e", ddl],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        logging.info("Hive tables created successfully")
        print("✅ Hive tables created: bronze.sales_streaming, silver.customer_spending, gold.customer_spending")
    else:
        logging.error(f"Failed to create Hive tables: {result.stderr}")
        raise Exception(f"Hive table creation failed: {result.stderr}")


# ── Task 5: Notify ───────────────────────────────────────────────────────────
def notify(**context):
    row_count = context['ti'].xcom_pull(key='row_count', task_ids='extract_task')
    logging.info(f"Pipeline completed! Processed {row_count} rows")
    print(f"✅ Pipeline hoàn thành — đã xử lý {row_count} rows")
    print(f"   Bronze : {HDFS_BASE}/data/bronze/streaming/sales")
    print(f"   Silver : {HDFS_BASE}/data/silver/customer_spending")
    print(f"   Gold   : {HDFS_BASE}/data/gold/customer_spending")


# ── DAG Definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id='sale_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline: Kafka → HDFS (Bronze/Silver/Gold) → Hive → Trino → Power BI',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
    )

    create_hive_tables_task = PythonOperator(
        task_id='create_hive_tables_task',
        python_callable=create_hive_tables,
    )

    notify_task = PythonOperator(
        task_id='notify_task',
        python_callable=notify,
    )

    # Dependency
    extract_task >> transform_task >> load_task >> create_hive_tables_task >> notify_task