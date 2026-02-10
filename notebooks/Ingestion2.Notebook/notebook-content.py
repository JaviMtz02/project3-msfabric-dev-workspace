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

# CELL ********************

# Importing Libraries
import requests
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, from_json, explode, first, collect_list
from pyspark.sql.types import *
from pyspark.sql import functions
import time
from openaq import OpenAQ
import json
from datetime import datetime, timedelta, timezone

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

country_ids_list = [
    155
]

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

openaq_conn = OpenAQ(api_key="7f633d8879ca8c19b8a7381094e5998f5e33d6adff5751929d4e588a34c1c0d5")
batch = []
max_date = None
# most_recent_date = None
table_created = False
counter = 1
window_days = 365

for id in country_ids_list:
    location_df = spark.read.format('delta').load(f'Files/Bronze/Locations/country_id={id}')

    sensor_ids = location_df.select(
        explode("sensors.id").alias("sensor_id"),
        col('dtfUTC')
    ).orderBy(location_df['dtfUTC'], ascending=True).na.drop(how="any")
    
    #if most_recent_date is None:
    # most_recent_date = sensor_ids.first().dtfUTC

    print(sensor_ids.count())

    for row in sensor_ids.toLocalIterator():
        sid = row.sensor_id
        dtf = datetime.fromisoformat(row.dtfUTC.replace("Z", "+00:00"))

        while True:
            dtt = dtf + timedelta(days=window_days)

            datetime_from_str = dtf.strftime('%Y-%m-%d %H:%M:%S')
            datetime_to_str = dtt.strftime('%Y-%m-%d %H:%M:%S') 
            
            try:
                resp = openaq_conn.measurements.list(sensors_id=sid, data='years', datetime_from=datetime_from_str, datetime_to=datetime_to_str, limit=1000)
                break
            except TimeoutError:
                window_days = window_days // 2

        if dtt > datetime.now(timezone.utc):
            if max_date is not None:
                most_recent_date = max_date
                max_date = None

            continue

        results = resp.results

        if resp.meta.found == '0' or len(results) == 0:
            time.sleep(1)
            continue

        for measurement in results:

            cur = measurement.period.datetime_from.utc

            if max_date is None or cur > max_date:
                max_date = cur

            batch.append(
                Row(
                    country_id = id,
                    sensor_id = sid,
                    pollutant_abbrev = measurement.parameter.name,
                    pollutant_name = pollutant_name.get(measurement.parameter.name, "Unknown"),
                    value = measurement.value,
                    units = measurement.parameter.units,
                    units_description = units_description.get(measurement.parameter.name, "Mass concentration of particles in micrograms per cubic meter"),
                    start_utc = measurement.period.datetime_from.utc,
                    end_utc = measurement.period.datetime_to.utc,
                    start_local = measurement.period.datetime_from.local,
                    end_local = measurement.period.datetime_to.local,
                    interval = measurement.period.label
                )
            )

        print('Batch Size:', len(batch))

        if len(batch) >= 100:
            df = spark.createDataFrame(batch)
            if not table_created:
                df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("country_id") \
                .save('Files/Bronze/Sensors')
                table_created = True
                
                print(f"Written Total Rows <> {counter * 100}")
            else:
                df.write \
                .format("delta") \
                .mode("append") \
                .save('Files/Bronze/Sensors')
                counter += 1
                print(f"Written total Rows <> {counter * 100}")
            
            batch = []
        
        # Conforming to API rate limits with 1 second delay between each request
        # Update most recent date from Python tracker
        if max_date is not None:
            most_recent_date = max_date
            max_date = None

        time.sleep(1)
    
    if len(batch) > 0:
        df = spark.createDataFrame(batch)

        if not table_created:
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("country_id") \
                .save('Files/Bronze/Sensors')
            table_created = True
        else:
            df.write \
                .format("delta") \
                .mode("append") \
                .save('Files/Bronze/Sensors')

        print(f"Final batch written: {len(batch)} rows")
        batch = []


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
