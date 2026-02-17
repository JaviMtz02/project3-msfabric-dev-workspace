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
from pyspark.sql import functions as F
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_date = '2026-02-15'

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

for country_id in country_ids_list:

    # Load tables filtered by country_id
    parenttable = spark.read.format('delta').load('Tables/incremental_parenttable') \
        .filter((col('country_id') == country_id) & (col('client_day') == target_day_only)) \
        .withColumnRenamed('pollutant_type_id', 'pt_id') \
        .withColumnRenamed('country_id', 'parent_country_id')

    locationtable = spark.read.format('delta').load('Tables/locationtable') \
        .filter(col('country_id') == country_id)

    ownertable = spark.read.format('delta').load('Tables/ownerlookuptable') \
        .filter(col('country_id') == country_id)

    pollutanttable = spark.read.format('delta').load('Tables/pollutantlookuptable') \
        .filter(col('country_id') == country_id)

    providertable = spark.read.format('delta').load('Tables/providerlookuptable') \
        .filter(col('country_id') == country_id)

    # =========== fact_table ===========
    fact_table = parenttable.select(
        col('parent_country_id').cast("string"),
        col('record_id').cast("string"),
        col('sensor_id').cast("string"),
        col('location_id').cast("string"),
        col('provider_id').cast("string"),
        col('owner_id').cast("string"),
        col('pt_id').cast("string"),
        col('value').cast("double"),
        col('Reading_Start_UTC').cast("string"),
        col('Reading_End_UTC').cast("string"),
        col('Reading_Start_Local').cast("string"),
        col('Reading_End_Local').cast("string"),
        col('client_day')
    ).dropna(subset=['record_id', 'value'], how='any')

    fact_table.write \
        .format('delta') \
        .mode(wr_mode) \
        .partitionBy('client_day', 'parent_country_id') \
        .saveAsTable("incremental_fact_table")

    # =========== summary_area ===========
    summary_area = parenttable.join(locationtable, parenttable.location_id == locationtable.id, 'inner') \
        .select(
            col('parent_country_id').cast("string"),
            col('sensor_id').cast("string"),
            col('location_id').cast("string"),
            col('country_code').cast("string"),
            col('Uptime_Start').cast("string"),
            col('Uptime_End').cast("string"),
            col('coordinates_longitude').alias('Longitude').cast("double"),
            col('coordinates_latitude').alias('Latitude').cast("double"),
            col('client_day')
        ).dropDuplicates() \
         .dropna(subset=['sensor_id'])

    summary_area.write \
        .format('delta') \
        .mode(wr_mode) \
        .partitionBy('client_day', 'parent_country_id') \
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
        col('parent_country_id').cast("string"),
        col('country_name').cast("string"),
        col('pt_id').cast("string"),
        col('pollutant_name').cast("string"),
        col('value').cast("double"),
        col('units').cast("string"),                
        col('area').cast("string"),
        col('locality').cast("string"),
        col('timezone').cast("string"),
        col('Reading_Start_UTC').cast("string"),
        col('Reading_End_UTC').cast("string"),
        col('Reading_Start_Local').cast("string"),
        col('Reading_End_Local').cast("string"),
              
        col('client_day')
    ) \
    .withColumn('hour_utc', hour(col('Reading_Start_UTC'))) \
    .withColumn('hour_local', hour(col('Reading_Start_Local')))

    summary_pollutant_hour = joined \
        .groupBy(
            'parent_country_id',
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
        .partitionBy('client_day', 'parent_country_id') \
        .saveAsTable("summary_pollutant_hour")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
