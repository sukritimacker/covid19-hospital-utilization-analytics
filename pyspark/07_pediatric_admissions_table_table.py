# Query 1: Calculate the cumulative sum vaccinated doses over years

# In[ ]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, countDistinct
import requests
import pandas as pd
from io import StringIO
import warnings
from collections import Counter
from pyspark.sql.functions import col, when, isnan, sum , to_date, split, corr, date_format,concat,lit
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import tqdm
import sys

# Create SparkSession
spark = SparkSession.builder.config(
    "temporaryGcsBucket", sys.argv[1]
).getOrCreate()

df = spark.read.format("bigquery").option("table", "raw_dataset.raw_data_cleaned").load()


# In[ ]:


df = df.withColumn('collection_week', to_date(col('collection_week'), 'yyyy/MM/dd'))


# In[ ]:


df = df.withColumn("year", split("collection_week", "-")[0].cast("int"))


# In[ ]:


df = df.withColumn("month", split("collection_week", "-")[1].cast("int"))
df = df.withColumn("month&year", concat(df["year"], lit("-"), df["month"]))


# In[ ]:


pediatric_admissions = df.select(
    date_format("collection_week", "yyyy-MM").alias("month_year"),
    "previous_day_admission_pediatric_covid_confirmed_0_4_7_day_sum",
    "previous_day_admission_pediatric_covid_confirmed_5_11_7_day_sum",
    "previous_day_admission_pediatric_covid_confirmed_12_17_7_day_sum"
)

# Grouping data by month and year to aggregate admissions
pediatric_admissions_by_month = pediatric_admissions.groupBy("month_year").sum()

# Renaming columns for better clarity
pediatric_admissions_by_month = pediatric_admissions_by_month.withColumnRenamed(
    "sum(previous_day_admission_pediatric_covid_confirmed_0_4_7_day_sum)", "age_0_4"
).withColumnRenamed(
    "sum(previous_day_admission_pediatric_covid_confirmed_5_11_7_day_sum)", "age_5_11"
).withColumnRenamed(
    "sum(previous_day_admission_pediatric_covid_confirmed_12_17_7_day_sum)", "age_12_17"
)
pediatric_admissions_by_month = pediatric_admissions_by_month.filter(col('age_0_4').isNotNull() & \
                    col('age_5_11').isNotNull() & col('age_12_17').isNotNull() )


# In[ ]:


# pediatric_admissions_by_month.write.format("bigquery").option(
#     "table", "prod_dataset.pediatric_admissions_table"
# ).mode("overwrite").save()

