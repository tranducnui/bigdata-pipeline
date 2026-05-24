from pyspark.sql.functions import *

def transform_data(df):
    result = (df.groupby("customer")
              .agg(sum("amount").alias('total_spent'))
              .orderBy(col("total_spent").desc()))
    return result