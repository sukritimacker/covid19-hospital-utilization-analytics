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





adult_patients_data = df.select(
    'state',
    'total_adult_patients_hospitalized_confirmed_covid_7_day_avg',
    'is_metro_micro'
)

# Group by state and is_metro_micro, and calculate the total confirmed adult patients
total_confirmed_adult_patients = adult_patients_data.groupBy('state', 'is_metro_micro') \
    .agg(F.sum('total_adult_patients_hospitalized_confirmed_covid_7_day_avg')
         .alias('total_confirmed_adult_patients'))

# Show the result
# total_confirmed_adult_patients.show()





total_confirmed_adult_patients_withStates = total_confirmed_adult_patients.join(state_df, total_confirmed_adult_patients['state'] == state_df['State_Abbreviation'], 'inner')





total_confirmed_adult_patients_withStates.write.format("bigquery").option(
    "table", "prod_dataset.metro_micro_table"
).mode("overwrite").save()































