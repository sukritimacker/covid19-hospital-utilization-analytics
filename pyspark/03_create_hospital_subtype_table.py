import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, countDistinct
import sys

# Create SparkSession
spark = SparkSession.builder.config(
    "temporaryGcsBucket", sys.argv[1]
).getOrCreate()

df = spark.read.format("bigquery").option("table", "raw_dataset.raw_data").load()

df = df.withColumn("year", split("collection_week", "/")[0])

df = df.groupby(["year", "state", "hospital_subtype"]).agg(countDistinct("hospital_name").alias("count"))

df.write.format("bigquery").option(
    "table", "prod_dataset.hospital_subtype_table"
).mode("overwrite").save()