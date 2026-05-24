from pyspark.sql import SparkSession
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
import logging

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Pipeline started")

spark = (SparkSession.builder.
         appName("SalesETL").
         master("local[*]").
         getOrCreate())

#extact
df = extract_data(spark)

#transform
result = transform_data(df)

#show result
result.show()

#load
load_data(result)

logging.info("Pipeline completed successfully")

spark.stop()