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
from pyspark.sql.functions import col, when, isnan, sum as spark_sum , to_date, split, corr, date_format,concat,lit
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import tqdm
import sys
# Create SparkSession
spark = SparkSession.builder.config(
    "temporaryGcsBucket", sys.argv[1]
).getOrCreate()

df = spark.read.format("bigquery").option("table", "raw_dataset.raw_data").load()


# In[ ]:


df = df.withColumn('collection_week', to_date(col('collection_week'), 'yyyy/MM/dd'))


# In[ ]:


df = df.withColumn("year", split("collection_week", "-")[0].cast("int"))


# In[ ]:


df = df.withColumn("month", split("collection_week", "-")[1].cast("int"))
df = df.withColumn("month&year", concat(df["year"], lit("-"), df["month"]))


# In[ ]:


windowSpec = Window.partitionBy('year').orderBy('month').rowsBetween(Window.unboundedPreceding, 0)
data_as = df.filter((col('previous_day_admission_adult_covid_confirmed_7_day_sum').isNotNull()) & (col('previous_day_admission_adult_covid_confirmed_7_day_sum') != -999999.0) &   (col('total_personnel_covid_vaccinated_doses_all_7_day') != -999999.0) &  (col('total_personnel_covid_vaccinated_doses_all_7_day').isNotNull())) 

admission_vaccination_df = data_as \
    .withColumn('cumulative_vaccinated_doses', spark_sum('total_personnel_covid_vaccinated_doses_all_7_day') \
                .over(windowSpec)) \
    .select('year', 'month', 'cumulative_vaccinated_doses')


# In[ ]:


admission_vaccination_df.write.format("bigquery").option(
    "table", "prod_dataset.vaccination_doses_timeline_table"
).mode("overwrite").save()

