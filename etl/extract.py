from pyspark.sql import SparkSession

def extract_data(spark: SparkSession):
    df = spark.read.csv(
        "data/sales.csv",
        header=True,
        inferSchema=True
    )
    return df