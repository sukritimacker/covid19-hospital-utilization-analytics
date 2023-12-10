



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





bed_utilization_columns = [
    'year',
    'state',
    'total_beds_7_day_avg',
    'inpatient_beds_used_7_day_avg',
    'total_icu_beds_7_day_avg',
    'icu_beds_used_7_day_avg'
]

# Select the necessary columns from the DataFrame
bed_utilization_data = df.select(bed_utilization_columns)

# Group by 'collection_week', 'state', and calculate the sum of bed utilization over the weeks for each state
utilized_beds_by_state = bed_utilization_data.groupBy('year', 'state').agg(
    F.sum('total_beds_7_day_avg').alias('total_beds_utilized'),
    F.sum('inpatient_beds_used_7_day_avg').alias('inpatient_beds_utilized'),
    F.sum('total_icu_beds_7_day_avg').alias('total_icu_beds_utilized'),
    F.sum('icu_beds_used_7_day_avg').alias('icu_beds_utilized')
)

# Show the aggregated results
# utilized_beds_by_state.show()





bed_utli_states = utilized_beds_by_state.join(state_df, utilized_beds_by_state['state'] == state_df['State_Abbreviation'], 'inner')





# bed_utli_states.write.format("bigquery").option(
#     "table", "prod_dataset.icu_bed_utilization_table"
# ).mode("overwrite").save()



















