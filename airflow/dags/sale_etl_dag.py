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

# ── Task 1: Extract ──────────────────────────────
def extract(**context):
    from etl.extract import extract_data
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
             .appName("SalesETL")
             .master("local[*]")
             .getOrCreate())

    df = extract_data(spark)
    count = df.count()
    logging.info(f"Extracted {count} rows")

    # Lưu tạm vào file để task sau dùng
    df.write.mode("overwrite").parquet("/opt/airflow/data/extracted")
    context['ti'].xcom_push(key='row_count', value=count)

# ── Task 2: Transform ────────────────────────────
def transform(**context):
    from etl.transform import transform_data
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
             .appName("SalesETL")
             .master("local[*]")
             .getOrCreate())

    df = spark.read.parquet("/opt/airflow/data/extracted")
    result = transform_data(df)
    result.show()

    result.write.mode("overwrite").parquet("/opt/airflow/data/transformed")
    logging.info("Transform completed")

# ── Task 3: Load ─────────────────────────────────
def load(**context):
    from etl.load import load_data
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
             .appName("SalesETL")
             .master("local[*]")
             .getOrCreate())

    result = spark.read.parquet("/opt/airflow/data/transformed")
    load_data(result)
    logging.info("Load completed")

# ── Task 4: Notify ───────────────────────────────
def notify(**context):
    row_count = context['ti'].xcom_pull(key='row_count', task_ids='extract_task')
    logging.info(f"Pipeline completed! Processed {row_count} rows")
    print(f"✅ Pipeline hoàn thành — đã xử lý {row_count} rows")

# ── DAG Definition ───────────────────────────────
with DAG(
    dag_id='sale_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline: CSV -> PySpark -> SQL Server',
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

    notify_task = PythonOperator(
        task_id='notify_task',
        python_callable=notify,
    )

    # Dependency
    extract_task >> transform_task >> load_task >> notify_task