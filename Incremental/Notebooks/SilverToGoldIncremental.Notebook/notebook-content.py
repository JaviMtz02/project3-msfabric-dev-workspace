# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a580ad55-0f08-47c7-9c2f-761283916f8f",
# META       "default_lakehouse_name": "Ingestion_Lakehouse",
# META       "default_lakehouse_workspace_id": "38ea5278-6006-410d-8a76-80fa8b39e1e9",
# META       "known_lakehouses": [
# META         {
# META           "id": "a580ad55-0f08-47c7-9c2f-761283916f8f"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "12fad281-acba-9ea1-4589-fc8bc7e5412a",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # KPIs
# separate dashboard for global, batch, incremental
# 
# ### Global
# - Pollutant change for each type over time in the world per year
# - Plot pollution over population density map
# - List of countrys from most to least polluted
# - Statistics on values over time (std dev, max, min etc)
# 
# ### Country
# - Pollutant change for each type over time per country and world per year
# - Active sensors over time per country
# - Statistics on values over time (std dev, max, min etc) per country
# - Best and worst pollution by locality, continent, hemisphere, timezone
# - Top providers by country
# 
# ### Hourly
# - Temperature vs pollutant type
# - Which hour has the highest activity (Peak Pollution Window)
# - Longest Streak of bad AQ
# - Nighttime recovery rate

# CELL ********************

# Overwrites only partitions present in df
# Leaves all other partitions untouched
# Prevents accidental full-table wipes

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PySpark
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, StringType,
    DoubleType, ArrayType
)
from pyspark.sql.functions import *

# Date & Time
from datetime import datetime, timedelta, timezone, date, time as dt_time
import time

# Data & API
import requests
import json
import pandas as pd
import numpy as np
from openaq import OpenAQ

# Language Processing
from pypinyin import pinyin, lazy_pinyin, Style
import pykakasi

# Validation
from math import fabs

from delta.tables import DeltaTable
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_date = '2026-02-18'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_day_only = date.fromisoformat(target_date.replace("/", "-"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

country_ids_list = [
    10, # China
    155, # USA
    9, # India 
    45, # Brazil 
    177, # Australia 
    50, # Germany 
    190, # Japan  
    99, # Senegal
    37, # South Africa 
    192 # Iceland  
]

def to_pinyin(text):
    if not text:
        return "Unknown"
    return (" ".join(lazy_pinyin(text, v_to_u=True))).title()

def to_romaji(text):
    if not text:
        return "Unknown"
    kks = pykakasi.kakasi()
    result = kks.convert(text)
    return " ".join([item['hepburn'] for item in result]).title()

lp_udf = udf(to_pinyin, StringType())
rm_udf = udf(to_romaji, StringType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

wr_mode = 'overwrite'
tz_strip_regex = r"^(.+?)(?:Z|[+-]\d{2}:\d{2})$"

for country_id in country_ids_list:

    # Load tables filtered by country_id
    parenttable = spark.read.format('delta').load('Tables/incremental_parenttable') \
        .filter((col('country_id') == country_id) & (col('client_day') == target_day_only)) \
        .withColumnRenamed('pollutant_type_id', 'pt_id') \

    locationtable = spark.read.format('delta').load('Tables/locationtable') \
        .filter(col('country_id') == country_id)

    ownertable = spark.read.format('delta').load('Tables/ownerlookuptable') \
        .filter(col('country_id') == country_id)

    pollutanttable = spark.read.format('delta').load('Tables/pollutantlookuptable') \
        .filter(col('country_id') == country_id)

    providertable = spark.read.format('delta').load('Tables/providerlookuptable') \
        .filter(col('country_id') == country_id)

    # =========== fact_table ===========
    fact_table = parenttable.join(locationtable, parenttable.location_id == locationtable.id, 'inner') \
        .select(
            col('country_name').cast("string"),
            col('record_id').cast("string"),
            col('sensor_id').cast("string"),
            col('location_id').cast("string"),
            col('provider_id').cast("string"),
            col('owner_id').cast("string"),
            col('pt_id').cast("string"),
            col('value').cast("double"),
            col('Reading_Start_UTC').cast("timestamp"),
            col('Reading_End_UTC').cast("timestamp"),
            regexp_extract(col('Reading_Start_Local'), tz_strip_regex, 1).cast("timestamp").alias("Reading_Start_Local"),
            regexp_extract(col('Reading_End_Local'), tz_strip_regex, 1).cast("timestamp").alias("Reading_End_Local"),
            col('client_day')
        ).dropna(subset=['record_id', 'value'], how='any')

    fact_table.write \
        .format('delta') \
        .mode(wr_mode) \
        .partitionBy('client_day', 'country_name') \
        .saveAsTable("incremental_fact_table")

    # =========== summary_area ===========
    summary_area = parenttable.join(locationtable, parenttable.location_id == locationtable.id, 'inner') \
        .select(
            col('country_name').cast("string"),
            col('country_code').cast("string"),
            col('sensor_id').cast("string"),
            col('location_id').cast("string"),
            col('Uptime_Start').cast("timestamp"),
            col('Uptime_End').cast("timestamp"),
            col('coordinates_longitude').alias('Longitude').cast("double"),
            col('coordinates_latitude').alias('Latitude').cast("double"),
            col('client_day')
        ).dropDuplicates() \
         .dropna(subset=['sensor_id'])

    summary_area.write \
        .format('delta') \
        .mode(wr_mode) \
        .partitionBy('client_day', 'country_name') \
        .saveAsTable("incremental_summary_area")

    # =========== summary_pollutant_hour ===========
    joined = parenttable.join(
        pollutanttable,
        parenttable.pt_id == pollutanttable.pollutant_type_id,
        'inner'
    ) \
    .join(
        locationtable,
        locationtable.id == parenttable.location_id,
        'inner'
    ) \
    .select(
        col('sensor_id').cast("string"),
        col('location_id').cast("string"),
        col('country_name').cast("string"),
        col('pt_id').cast("string"),
        col('pollutant_name').cast("string"),
        col('value').cast("double"),
        col('units').cast("string"),                
        col('area').cast("string"),
        col('locality').cast("string"),
        col('timezone').cast("string"),
        col('Reading_Start_UTC').cast("timestamp"),
        col('Reading_End_UTC').cast("timestamp"),
        regexp_extract(col('Reading_Start_Local'), tz_strip_regex, 1).cast("timestamp").alias("Reading_Start_Local"),
        regexp_extract(col('Reading_End_Local'), tz_strip_regex, 1).cast("timestamp").alias("Reading_End_Local"),
        col('client_day')
    ) \
    .withColumn('hour_utc', date_format(to_timestamp(col("Reading_Start_UTC")), "H").cast("int")) \
    .withColumn('hour_local', date_format(to_timestamp(col("Reading_Start_Local")), "H").cast("int")) \


    summary_pollutant_hour = joined \
        .groupBy(
            'country_name',
            'area',
            'locality',
            'timezone',
            'hour_utc',
            'hour_local',
            'units',
            'pollutant_name',
            'pt_id',
            'client_day'

        ) \
        .agg(avg(col('value')).alias('value')) \
        .sort('hour_local') \
        .withColumn(
            'Area',
            when(col('country_name') == 'China', lp_udf(col('area')))
            .when(col('country_name') == 'Japan', rm_udf(col('area')))
            .otherwise(col('area'))
        )

    summary_pollutant_hour.write \
        .format('delta') \
        .mode(wr_mode) \
        .partitionBy('client_day', 'country_name') \
        .saveAsTable("summary_pollutant_hour")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation

fact_today = spark.read.format('delta') \
                  .load('Tables/incremental_fact_table') \
                  .filter(col('client_day') == target_day_only) \
                  .select(col('record_id')) \
                  .distinct()

sum_area_today = spark.read.format('delta') \
                           .load('Tables/incremental_summary_area') \
                           .filter(col('client_day') == target_day_only) \
                           .select(col('sensor_id')) \
                           .distinct()

fact_yesterday = spark.read.format('delta') \
                  .load('Tables/incremental_fact_table') \
                  .filter(col('client_day') == (target_day_only - timedelta(days=1))) \
                  .select(col('record_id')) \
                  .distinct()

sum_area_yesterday = spark.read.format('delta') \
                           .load('Tables/incremental_summary_area') \
                           .filter(col('client_day') == (target_day_only - timedelta(days=1))) \
                           .select(col('sensor_id')) \
                           .distinct()

value1 = fact_today.count()
value2 = sum_area_today.count()
value4 = fact_yesterday.count()
value5 = sum_area_yesterday.count()

if fabs(value1 - value4) / fabs(value4) <= (10 / 100):
    pass
else:
    raise Exception('Todays counts not within 10% of yesterdays')

if fabs(value2 - value5) / fabs(value5) <= (10 / 100):
    pass
else:
    raise Exception('Todays counts not within 10% of yesterdays')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### # Sensors read from

# CELL ********************

target_summary_table = "sensor_count_daily"

# 1. Group by day, country, AND pollutant type
sensor_count_daily = spark.read.table("incremental_fact_table") \
    .filter(col("client_day") == target_day_only) \
    .groupBy("client_day", 'country_name', "pt_id") \
    .agg(countDistinct("sensor_id").alias("total_sensors"))

if spark.catalog.tableExists(target_summary_table):
    delta_target = DeltaTable.forName(spark, target_summary_table)
    
    # 2. Update merge condition to include pt_id
    # This ensures uniqueness for each pollutant within a country per day
    delta_target.alias("target").merge(
        sensor_count_daily.alias("updates"),
        """
        target.client_day = updates.client_day AND 
        target.country_name = updates.country_name AND 
        target.pt_id = updates.pt_id
        """
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    
else:
    sensor_count_daily.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("client_day", 'country_name') \
        .saveAsTable(target_summary_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### # of Providers

# CELL ********************

target_summary_table = "provider_count_daily"

# 1. Group by day, country, AND pollutant type
provider_count_daily = spark.read.table("incremental_fact_table") \
    .filter(col("client_day") == target_day_only) \
    .groupBy("client_day", 'country_name', "pt_id") \
    .agg(countDistinct("provider_id").alias("total_providers"))

if spark.catalog.tableExists(target_summary_table):
    delta_target = DeltaTable.forName(spark, target_summary_table)
    
    # 2. Update merge condition to include pt_id
    # This ensures uniqueness for each pollutant within a country per day
    delta_target.alias("target").merge(
        provider_count_daily.alias("updates"),
        """
        target.client_day = updates.client_day AND 
        target.country_name = updates.country_name AND 
        target.pt_id = updates.pt_id
        """
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    
else:
    provider_count_daily.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("client_day", 'country_name') \
        .saveAsTable(target_summary_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### DoD Change

# CELL ********************

# =========== dod_pollutant_analytics ===========
target_summary_table = "dod_pollutant_analytics"

# 1. Performance Optimization: Only read the last 2-3 days of data.
# This provides enough history for the LAG() to work without scanning the entire table.
summary_df = spark.read.table("summary_pollutant_hour") \
    .filter(col("client_day") >= date_sub(to_date(lit(target_day_only)), 2))

# 2. Define Window: 
dod_window = Window.partitionBy('country_name', "pt_id") \
                   .orderBy("client_day") 

# 3. Aggregate and Calculate DoD
dod_change_df = summary_df.groupBy(
    "client_day", 'country_name', "pt_id", "pollutant_name"
).agg(
    avg("value").alias("avg_value"),
    min("value").alias("min_value"),
    max("value").alias("max_value")
).withColumn(
    "prev_avg_value", lag("avg_value").over(dod_window)
).withColumn(
    "dod_pct_change", 
    ((col("avg_value") - col("prev_avg_value")) / col("prev_avg_value")) * 100
)

# 4. Filter for only the specific day we are processing
# The lag calculation happens BEFORE this filter, so the previous day is still "seen"
incremental_updates = dod_change_df.filter(col("client_day") == target_day_only)

# 5. Delta Merge (Upsert)
if spark.catalog.tableExists(target_summary_table):
    delta_target = DeltaTable.forName(spark, target_summary_table)
    
    delta_target.alias("target").merge(
        incremental_updates.alias("updates"),
        """
        target.client_day = updates.client_day AND 
        target.country_name = updates.country_name AND 
        target.pt_id = updates.pt_id
        """
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
else:
    incremental_updates.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("client_day", 'country_name') \
        .saveAsTable(target_summary_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Top 3 Problem Areas

# CELL ********************

# Top 3 most polluted areas by each country by pollutant
target_summary_table = 'daily_top_polluted_areas'

# 1. Getting tables
joined_incremental_fact_table = spark.read.table('incremental_fact_table')
dim_location = spark.read.table('dim_location') \
                         .select(
                            col('location_id'),
                            col('area'),
                            col('locality'),
                            col('timezone'),
                            col('country_name').alias('country')
                         )
                         
dim_pollutant = spark.read.table('dim_pollutant')


# Performing aggregation
joined_incremental_fact_table = joined_incremental_fact_table.filter(col("client_day") == target_day_only)

joined_incremental_fact_table = joined_incremental_fact_table \
            .join(dim_location,joined_incremental_fact_table.location_id == dim_location.location_id,'inner') \
            .drop('location_id')

joined_incremental_fact_table = joined_incremental_fact_table \
            .join(dim_pollutant,joined_incremental_fact_table.pt_id == dim_pollutant.pollutant_type_id,'inner') \
            .drop('pollutant_type_id')

joined_incremental_fact_table = joined_incremental_fact_table \
                .select('client_day', 'country_name', 'record_id','value','pt_id','pollutant_name','area','locality','timezone') \
                .groupBy('client_day', 'country_name','area','pt_id').agg(sum('value').alias('total_value'))

window_spec = Window.partitionBy('country_name', "pt_id") \
                    .orderBy(desc("total_value"))

ranked_df = joined_incremental_fact_table.withColumn(
    "rank",
    row_number().over(window_spec)
)

top_3_areas_df = ranked_df.filter(col("rank") <= 3)



# Not writing the correct table 
if spark.catalog.tableExists(target_summary_table):
    delta_target = DeltaTable.forName(spark, target_summary_table)
    
    delta_target.alias("target").merge(
        top_3_areas_df.alias("updates"),
        """
        target.client_day = updates.client_day AND 
        target.country_name = updates.country_name AND 
        target.pt_id = updates.pt_id
        """
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
else:
    top_3_areas_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("client_day", 'country_name') \
        .saveAsTable(target_summary_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Rank

# CELL ********************

# Country rankings
target_summary_table = 'daily_country_pollutant_rank' 

fact_table = spark.read.table('incremental_fact_table')

dim_location = spark.read.table('dim_location') \
                         .select(
                            col('location_id'),
                            col('area'),
                            col('locality'),
                            col('timezone'),
                            col('country_name').alias('country')
                         )


dim_pollutant = spark.read.table('dim_pollutant')

joined_incremental_fact_table = fact_table.filter(col("client_day") == target_day_only)

joined_incremental_fact_table = joined_incremental_fact_table \
            .join(dim_location,joined_incremental_fact_table.location_id == dim_location.location_id,'inner') \
            .drop('location_id')

joined_incremental_fact_table = joined_incremental_fact_table \
            .join(dim_pollutant,joined_incremental_fact_table.pt_id == dim_pollutant.pollutant_type_id,'inner') \
            .drop('pollutant_type_id')

joined_incremental_fact_table = joined_incremental_fact_table \
                .select('client_day', 'country_name', 'record_id','value','pt_id','pollutant_name','locality','timezone') \
                .groupBy('client_day', 'country_name','pt_id').agg(sum('value').alias('total_value'))


window_spec = Window.partitionBy("pt_id") \
                    .orderBy(desc("total_value"))

ranked_df = joined_incremental_fact_table.withColumn(
    "rank",
    rank().over(window_spec)
)

# display(ranked_df)

# ===============================
# Upsert into Delta Table
# ===============================
if spark.catalog.tableExists(target_summary_table):

    delta_target = DeltaTable.forName(spark, target_summary_table)

    delta_target.alias("target").merge(
        ranked_df.alias("updates"),
        """
        target.client_day = updates.client_day AND
        target.country_name = updates.country_name AND
        target.pt_id = updates.pt_id
        """
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    ranked_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("client_day", 'country_name') \
        .saveAsTable(target_summary_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Number of violations

# CELL ********************

target_summary_table = 'daily_pollutant_threshhold'
fact_table = spark.read.table('incremental_fact_table').filter(col("client_day") == target_day_only)

# If average pollutant is over a certain number

consersions = {
    'pm25' : 15,
    'pm25-old' : 15,
    'pm10' : 45,
    'no2' : 25,
    'o3' : 100,
    'so2' : 40,
    'co' : 4,
    'pm1' : 15,
    'nox' : 25,
    'no': 25,
    'bc' : 3,
    'co2' : 4000,
    'um003' : 20000,
    'wind_direction' : 999999,
    'pressure' : 999999,
    'relativehumidity' : 999999,
    'temperature' : 999999,
    'wind_speed' : 999999
}

threshold_map = create_map(
    *[x for k, v in consersions.items() for x in (lit(k), lit(float(v)))]
)

threshold = (
    fact_table
    .withColumn("threshold", threshold_map[col("pt_id")])
    .withColumn(
        "above_threshold",
         when(col("threshold").isNotNull(),
               col("value") > col("threshold"))
         .otherwise(lit(False))
    )
    .drop("threshold")
)

threshold = threshold.select('country_name', 'pt_id', 'value', 'client_day', 'above_threshold')


if spark.catalog.tableExists(target_summary_table):

    delta_target = DeltaTable.forName(spark, target_summary_table)

    delta_target.alias("target").merge(
        threshold.alias("updates"),
        """
        target.country_name = updates.country_name AND
        target.pt_id = updates.pt_id AND
        target.value = updates.value AND
        target.client_day = updates.client_day AND
        target.above_threshold = updates.above_threshold
        """
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

else:
    threshold.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("client_day", 'country_name') \
        .saveAsTable(target_summary_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
