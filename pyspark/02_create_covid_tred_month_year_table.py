


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, countDistinct
import requests
import pandas as pd
from io import StringIO
import warnings
from collections import Counter
from pyspark.sql.functions import col, when, isnan, sum as spark_sum, to_date, split, lit
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import tqdm
from pyspark.sql.types import StructType, StructField, StringType
import sys




# Create SparkSession
spark = SparkSession.builder.config(
    "temporaryGcsBucket", sys.argv[1]
).getOrCreate()

df = spark.read.format("bigquery").option("table", "raw_dataset.raw_data_cleaned").load()





df.createOrReplaceTempView("sum_covid")





# Define the schema for the DataFrame
schema = StructType([
    StructField("Full_Name", StringType(), False),
    StructField("State_Abbreviation", StringType(), False)
])

# Data for full names and abbreviations of US States
us_states = [
    ("Alabama", "AL"),
    ("Alaska", "AK"),
    ("Arizona", "AZ"),
    ("Arkansas", "AR"),
    ("California", "CA"),
    ("Colorado", "CO"),
    ("Connecticut", "CT"),
    ("Delaware", "DE"),
    ("Florida", "FL"),
    ("Georgia", "GA"),
    ("Hawaii", "HI"),
    ("Idaho", "ID"),
    ("Illinois", "IL"),
    ("Indiana", "IN"),
    ("Iowa", "IA"),
    ("Kansas", "KS"),
    ("Kentucky", "KY"),
    ("Louisiana", "LA"),
    ("Maine", "ME"),
    ("Maryland", "MD"),
    ("Massachusetts", "MA"),
    ("Michigan", "MI"),
    ("Minnesota", "MN"),
    ("Mississippi", "MS"),
    ("Missouri", "MO"),
    ("Montana", "MT"),
    ("Nebraska", "NE"),
    ("Nevada", "NV"),
    ("New Hampshire", "NH"),
    ("New Jersey", "NJ"),
    ("New Mexico", "NM"),
    ("New York", "NY"),
    ("North Carolina", "NC"),
    ("North Dakota", "ND"),
    ("Ohio", "OH"),
    ("Oklahoma", "OK"),
    ("Oregon", "OR"),
    ("Pennsylvania", "PA"),
    ("Rhode Island", "RI"),
    ("South Carolina", "SC"),
    ("South Dakota", "SD"),
    ("Tennessee", "TN"),
    ("Texas", "TX"),
    ("Utah", "UT"),
    ("Vermont", "VT"),
    ("Virginia", "VA"),
    ("Washington", "WA"),
    ("West Virginia", "WV"),
    ("Wisconsin", "WI"),
    ("Wyoming", "WY")
]

# Create a DataFrame using the defined schema and state data
state_df = spark.createDataFrame(us_states, schema)





df = df.withColumn("year", split("collection_week", "-")[0].cast("int"))





df = df.withColumn("month", split("collection_week", "-")[1].cast("int"))





# data_as = df.filter(df['state'] == 'AS')
data_as = df.filter((col('total_adult_patients_hospitalized_confirmed_covid_7_day_avg').isNotNull()) & (col('total_adult_patients_hospitalized_confirmed_covid_7_day_avg') != -999999.0))

# changed here added alias
# Grouping data by 'year', 'month', and 'state' and calculating the sum of COVID-19 confirmed patients for each month for state 'AS'
sum_covid_monthly_as = data_as.groupBy('year', 'month', 'state').agg(F.sum('total_adult_patients_hospitalized_confirmed_covid_7_day_avg').alias("count"))

# Sorting the resulting DataFrame by 'year', 'month', and 'state' in ascending order
sum_covid_monthly_as = sum_covid_monthly_as.orderBy('year', 'month')

# Show the resulting DataFrame
# sum_covid_monthly_as.show()





sum_covid_monthly_withStates = sum_covid_monthly_as.join(state_df, sum_covid_monthly_as['state'] == state_df['State_Abbreviation'], 'inner')





sum_covid_monthly_withStates.write.format("bigquery").option(
    "table", "prod_dataset.covid_tred_month_year_table"
).mode("overwrite").save()

