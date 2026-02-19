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

# # OpenAQ API EDA
# <hr>

# CELL ********************

# Importing Libraries
from pyspark.sql.window import Window
import requests
import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions
import time
from openaq import OpenAQ
import json
from datetime import datetime, timedelta, timezone, time as dt_time # Rename for clarity
import time
from pyspark.sql import Row
import pyspark.sql.functions as F
from datetime import date, timedelta
from math import fabs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_date = '2026-02-13'

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

# Pollutant names
pollutant_name = {
    "pm25": "Particulate Matter ≤2.5µm",
    "pm10": "Particulate Matter ≤10µm",
    "so2": "Sulfur Dioxide",
    "o3": "Ozone",
    "co": "Carbon Monoxide",
    "bc": "Black Carbon",
    "no2": "Nitrogen Dioxide"
}

units_description = {
    "ppb": "Concentration in parts per billion"
}

key1 = '9232dfbe20d3408d94c5cd16c53ad7c779a0e9e3b55ac8684da02de13c776653' 
key2 = '1377214e9deac5c82546ff2173a791a07b796f6e588cb92a642a3c6d4b3477e1'
key3 = '6e02c2a5c01e0f3159f9975e367192454c06737364d218d3c669433932d1ede9'
key4 = '433dcbb8f48f1241cba2f72b731a66dd3d1d4491f556d3d33bf78b333c7b08d2'
key5 = 'ca9391621120b3952075d8978cf7e7495fa1ad42270742b399812998cbdcef7e'
key6 = 'ae4642ce5011144d2148279b9b73d5dcfa9eb21ec830d5c939b21371cfc71c37'
key7 = 'be61066aa290e31bd35cd0be3bfd5e73ca3290a8533d36c2c48df7bbd3cbc2e2'
key8 = 'f7fd6c45c7e737bd62aef1622cb9794f9227445c6a108f722f6672f78d69df0e'
key9 = 'ed4104818fce73e1645e835365033f9c75849adbdc07b8d5ffb40f531e02a295'
key10 = 'f49e32d1d7f1e547ef1da1fc5ec8db7791d52a36c6623b028904d8f3a3ab780c' 
key11 = '82136d93e47b6317f5efa89a2bf49383f53bce6fd531fa12c3f46803c8660982'
key12 = 'e7936c67c68a1fca13351727b23483cdc319366c98c10c9907e06f2cc058c999'
key13 = 'a54d4bba33c36bbfd034bc34180b9a3fd5afe46ec6d9dec4e94ce506beb58aea'
key14 = '7bfae171543a3af3903e5d99d93d2ed31ad3ff005f85457b944d08b8aaa3a77f'
key15 = '0eecd31da635e8cdce84ada910b388ec18c8b7b88da1268a41f774c8d85e205c'
key16 = '2f8c469e47e6741c58fd1eb1e6b532f49fad8df40464d48e95a3b5935c789047'
key17 = '6072d6300d2ccb4051bd0db13e23a7020abc639afa16dfa0bd4313659e1b9a6b'
key18 = '34f8ae1aea2f1468dcc10d28612d2e957df0f6eaeef1f0b548d3527a9c0c5dae'
key19 = 'ff3ac1a658aa3fd2e81b4a1cf196058360eccc53fc272e0333314e19decbf741'
key20 = 'ab859d77b2987ce8db8cccb7a9c7f4b300cd482c27fb093bbfea50c066b39520'
key21 = '4bc666803c2851e9a0b346fa6f26ae976b5ddb1d606a40db726511b6b29ac7ce'
key22 = '77ac990397b8df62f6626f8634fa7f46bacb98ec7e4c4e42cd255db23df12299'
key23 = '72d585582f88bd03f7e680b6f8b08c1d24eeb6ea870896d41bd5112ec1a60668'
key24 = 'a1747bc0ae24c5a165627017c920465b777032f16b0531c73fcf941c85bed001'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Fetch data from specified countries on a specific day

# CELL ********************

day_start = datetime.combine(target_day_only, dt_time.min).strftime('%Y-%m-%dT%H:%M:%S')
day_end = datetime.combine(target_day_only, dt_time.max).strftime('%Y-%m-%dT%H:%M:%S')

batch = []
sensors_path = 'Files/Bronze/Sensors'
table_path = 'Files/BronzeIncremental/Sensors'

connections = [
    OpenAQ(api_key=key1),
    OpenAQ(api_key=key2),
    OpenAQ(api_key=key3),
    OpenAQ(api_key=key4),
    OpenAQ(api_key=key5),
    OpenAQ(api_key=key6),
    OpenAQ(api_key=key7),
    OpenAQ(api_key=key8),
    OpenAQ(api_key=key9),
    OpenAQ(api_key=key10),
    OpenAQ(api_key=key11),
    OpenAQ(api_key=key12),
    OpenAQ(api_key=key13),
    OpenAQ(api_key=key14),
    OpenAQ(api_key=key15),
    OpenAQ(api_key=key16),
    OpenAQ(api_key=key17),
    OpenAQ(api_key=key18),
    OpenAQ(api_key=key19),
    OpenAQ(api_key=key20),
    OpenAQ(api_key=key21),
    OpenAQ(api_key=key22),
    OpenAQ(api_key=key23),
    OpenAQ(api_key=key24)
]

conn_index = 0

# 1. OPTIMIZATION: Get ALL active sensor IDs once.
print("Fetching all sensor IDs from Delta...")
all_sensors = spark.read.format('delta').load(sensors_path) \
    .filter(F.substring(F.col("end_local").cast("string"), 1, 4) == target_day_only.year) \
    .select(F.col("country_id"), F.col("sensor_id").alias("sid")) \
    .distinct() \
    .collect()

total_sensors = len(all_sensors)
print(f"Total sensors to process: {total_sensors}")

# Check if table exists
try:
    spark.read.format("delta").load(table_path)
    table_created = True
except:
    table_created = False

# 2. MAIN LOOP
for index, row in enumerate(all_sensors):
    sid = row.sid
    cid = row.country_id
    
    current_page = 1
    has_more_data = True
    
    while has_more_data:
        success = False
        retries = 0
        
        while not success:
            if retries > 2:
                print(f'Rate limit exceeded on sensor {sid} for country {cid}.')
                print(f'Failed key: {connections[conn_index]}')
                mssparkutils.notebook.exit('Failed')
            try:
                # REQUEST DATA
                resp = connections[conn_index].measurements.list(
                    sensors_id=sid,
                    data = 'hours',
                    datetime_from=day_start, 
                    datetime_to=day_end, 
                    limit=1000,
                    page=current_page
                )
                
                results = resp.results
                if results:
                    for m in results:
                        batch.append(Row(
                            country_id = cid,
                            sensor_id = sid,
                            pollutant_abbrev = m.parameter.name,
                            pollutant_name = pollutant_name.get(m.parameter.name, "Unknown"),
                            value = m.value,
                            units = m.parameter.units,
                            units_description = units_description.get(m.parameter.name, "N/A"),
                            start_utc = m.period.datetime_from.utc,
                            end_utc = m.period.datetime_to.utc,
                            start_local = m.period.datetime_from.local,
                            end_local = m.period.datetime_to.local,
                            interval = m.period.label
                        ))
                    
                    # If we got exactly 1000, there might be a next page
                    if len(results) == 1000:
                        current_page += 1
                    else:
                        has_more_data = False
                else:
                    has_more_data = False
                
                success = True
                time.sleep(.1) # Prevent bursting

                conn_index = (conn_index + 1) % len(connections) # Update which key to use for requests
                
            except Exception as e:
                if "Rate limit exceeded" in str(e):
                    retries += 1
                    print(f"Rate limit hit! Sensor {sid}. Waiting 60s (Retry {retries}/3)...")
                    time.sleep(60)
                else:
                    print(f"Error for sensor {sid}: {e}")
                    has_more_data = False # Skip if it's a permanent error
                    break

    # 3. BATCH WRITE (Flushing data periodically)
    if len(batch) >= 2000:
        df_batch = spark.createDataFrame(batch).withColumn("client_day", lit(target_day_only))
        mode = "append" if table_created else "overwrite"
        df_batch.write.format("delta").mode(mode).partitionBy("client_day", "country_id").save(table_path)
        table_created = True
        print(f"Progress: {index+1}/{total_sensors} sensors. Saved {len(batch)} rows.")
        batch = []

# Final flush
if batch:
    df_batch = spark.createDataFrame(batch).withColumn("client_day", lit(target_day_only))
    mode = "append" if table_created else "overwrite"
    df_batch.write.format("delta").mode(mode).partitionBy("client_day", "country_id").save(table_path)
    print(f"Final flush: {len(batch)} rows saved.")

print("Ingestion Complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation

sensors_today = spark.read.format('delta') \
                          .load(f'Files/BronzeIncremental/Sensors') \
                          .filter(col('client_day') == target_day_only) \
                          .select('sensor_id') \
                          .distinct()

sensors_yesterday = spark.read.format('delta') \
                              .load(f'Files/BronzeIncremental/Sensors') \
                              .filter(col('client_day') == (target_day_only - timedelta(days=1))) \
                              .select('sensor_id') \
                              .distinct()

value1 = sensors_today.count()
value2 = sensors_yesterday.count()

if fabs(value1 - value2) / fabs(value2) <= (10 / 100):
    pass
else:
    raise Exception('Todays sensor count not within 10% of yesterdays')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
