


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, countDistinct
import requests
import pandas as pd
from io import StringIO
import warnings
from collections import Counter
from pyspark.sql.functions import col, when, isnan, sum as spark_sum, to_date, split
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import tqdm
import sys




# Create SparkSession
spark = SparkSession.builder.config(
    "temporaryGcsBucket", sys.argv[1]
).getOrCreate()

df = spark.read.format("bigquery").option("table", "raw_dataset.raw_data").load()





df = df.withColumn('collection_week', to_date(col('collection_week'), 'yyyy/MM/dd'))





column_name = 'is_metro_micro'

# Cast the column to boolean type
df = df.withColumn(column_name, col(column_name).cast('boolean'))





columns = df.columns

# Convert columns starting from the 12th column to double
for col_name in columns[11:]:  # Considering columns start from index 0
    df = df.withColumn(col_name, col(col_name).cast("float"))





int_columns = ['fips_code', 'zip', 'hospital_pk']
for col_name in int_columns:
    df = df.withColumn(col_name, col(col_name).cast("int"))





def drop_null_columns(df, threshold=0.5):
    total_rows = df.count()
    for col_name in df.columns:
        null_count = df.where(col(col_name).isNull()).count()
        null_percentage = null_count / total_rows
        if null_percentage > threshold:
            df = df.drop(col_name)
    return df

# Applying the function to drop columns with more than 50% null values
df = drop_null_columns(df, threshold=0.5)





def replace_negative_outliers_with_median(df):
    for col_name in df.columns[11:]: 
        median_val = df.approxQuantile(col_name, [0.5], 0.001)[0]  # Calculating median
        df = df.withColumn(col_name, when(col(col_name) == -999999.0, median_val).otherwise(col(col_name)))
    return df

# Applying the function to replace -999999.0 with column median
df = replace_negative_outliers_with_median(df)





df.write.format("bigquery").option(
    "table", "raw_dataset.raw_data_cleaned"
).mode("overwrite").save()

