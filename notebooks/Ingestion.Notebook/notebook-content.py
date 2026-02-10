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

%pip install openaq

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Importing Libraries
from pyspark.sql.window import Window
import requests
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, from_json, explode, first, collect_list, row_number
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

# Country IDs
country_ids_list = [
    9,
    99
]

#Full list
# country_ids_list = [
    # 10, # China DONE
    # 155, # USA
    # 9, # India 
    # 45, # Brazil 
    # 177, # Australia 
    # 50, # Germany 
    # 190, # Japan  
    # 99, # Senegal DONE
    # 37, # South Africa 
    # 192 # Iceland  
# ]

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

# this is the basic url you'll need to get data
url = "https://api.openaq.org/v3/"

cade_key = 'cca79e3b9142f47741161303bd0848bcd1da45490c5c85394a35f83f1b27a2f6' # cybersecurity
javi_key = '93136dfbe2a3bbb99cbbeab6e7ea08b144cd7718823d4d35e2360b34170b7b37'

headers = {
    'X-API-KEY': cade_key
}

async def get_resp(group, params, page):
    temp = ''
    has_params = False

    if params[0] != '':
        has_params = True
        temp = group + '/'

        for i in params:
            temp += i + '/'
    
        temp = temp[:-1]
    else:
        temp = group

    cust_url = url + temp

    if page != '' and has_params == False:
        cust_url += f'?page={page}&limit=100'
    elif page != '' and has_params:
        cust_url = cust_url[:-1]
        cust_url += f'?page={page}&limit=100'

    print(cust_url)

    response = requests.get(cust_url, headers=headers)
    
    print('Status Code:', response.status_code)
    
    if response.ok:
        data = response.json()
        return data
    else:
        return 'invalid'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TOTAL_PAGES = 233
BATCH_SIZE = 10
table_created = False
buffer = []

for i in range(1, TOTAL_PAGES + 1):
    resp = await get_resp("locations", [""], i)
    buffer.extend(resp["results"])

    if i % BATCH_SIZE == 0 or i == TOTAL_PAGES:
        pdf = pd.json_normalize(buffer)
        df = spark.createDataFrame(pdf)

        final_df = df.select(
            col("id"),
            col("name").alias("area"),
            col("locality"),
            col("timezone"),
            col("instruments"),
            col("sensors"),
            col("bounds"),
            col("`datetimeFirst.utc`").alias("dtfUTC"),
            col("`datetimeLast.utc`").alias("dtlUTC"),
            col("`country.id`").alias("country_id"),
            col("`country.name`").alias("country_name"),
            col("`country.code`").alias("country_code"),
            col("`owner.id`").alias("owner_id"),
            col("`owner.name`").alias("owner_name"),
            col("`provider.id`").alias("provider_id"),
            col("`provider.name`").alias("provider_name"),
            col("`coordinates.longitude`").alias("coordinates_longitude"),
            col("`coordinates.latitude`").alias("coordinates_latitude")
        ).filter(col("country_id").isin(country_ids_list))

        if not table_created:
            # first batch: overwrite + partition
            final_df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("country_id") \
                .save('Files/Bronze/Locations')
            table_created = True
        else:
            # subsequent batches: append only
            final_df.write \
                .format("delta") \
                .mode("append") \
                .save('Files/Bronze/Locations')

        buffer.clear()
        
        # Conforming to API rate limits with 6 second delay between batch request
        time.sleep(6)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resp = openaq_conn.measurements.list(sensors_id=sid, data='years', datetime_from=datetime_from_str, datetime_to=datetime_to_str, limit=1000)
results = resp.results
batch = []
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
df = spark.createDataFrame(batch)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

openaq_conn = OpenAQ(api_key="93136dfbe2a3bbb99cbbeab6e7ea08b144cd7718823d4d35e2360b34170b7b37")
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
                print(row)
                window_days = window_days // 2
                time.sleep(60)

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

        time.sleep(2)
    
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

# MARKDOWN ********************

# # Notes
# <hr>
# 
# - Add functionality at the bottom to write the end of the batch since it will not be enough to trigger the 'write' if statement <strong> IN PROGRESS </strong>

# MARKDOWN ********************

# ### DROPS ALL TABLES, REMOVE FOR PRODUCTION

# CELL ********************

# tables = spark.catalog.listTables()

# for table in tables:
#     spark.sql(f"DROP TABLE IF EXISTS {table.name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Full list
# country_ids_list = [
#     10, # China DONE
#     155, # USA
#     9, # India 
#     45, # Brazil 
#     177, # Australia 
#     50, # Germany 
#     190, # Japan  
#     99, # Senegal DONE
#     37, # South Africa 
#     192 # Iceland DONE
# ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

location_df = spark.read.format('delta').load(f'Files/Bronze/Locations/country_id=37')

sensor_ids = location_df.select(
        explode("sensors.id").alias("sensor_id"),
        col('dtfUTC')
    ).orderBy(location_df['dtfUTC'], ascending=True).na.drop(how="any")

print(sensor_ids.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = Window.orderBy("dtfUTC")

df_rn = sensor_ids.withColumn("row_num", row_number().over(window_spec))
target_row = (
    df_rn
    .filter(df_rn.sensor_id == 14043983)
    .select("row_num")
    .limit(1)
    .collect()[0][0]
)

before_df = df_rn.filter(df_rn.row_num < target_row)
after_df = df_rn.filter(df_rn.row_num >= target_row)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(after_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Add logic for dtt check, so all dates are inclusive up to 2/5/2026
# 
# - Run javiers logic on US, China
# - Finish Senegal, India
# 
# US, India, Senegal, China

# CELL ********************

id = 37
openaq_conn = OpenAQ(api_key="cca79e3b9142f47741161303bd0848bcd1da45490c5c85394a35f83f1b27a2f6")
counter = 1
batch = []
window_days=365
table_created = False
max_date = None
for row in after_df.toLocalIterator():
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
            print(row)
            window_days = window_days // 2
            time.sleep(60)

    if dtt > datetime.now(timezone.utc):
        while dtt > datetime.now(timezone.utc):
            dtt -= timedelta(days=1)

        if max_date is not None:
            most_recent_date = max_date
            max_date = None

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

    print(f'Current row count: {len(batch)}')

    if len(batch) >= 100:
        df = spark.createDataFrame(batch)
        if not table_created:
            df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("country_id") \
            .save('Files/Bronze2/Sensors')
            table_created = True
            
            print(f"Written Total Rows <> {counter * 100} \n Batch Number: {counter}")
        else:
            df.write \
            .format("delta") \
            .mode("append") \
            .save('Files/Bronze2/Sensors')
            counter += 1
            print(f"Written total Rows <> {counter * 100} \n Batch Number: {counter}")
        
        batch = []
    
    # Conforming to API rate limits with 1 second delay between each request
    # Update most recent date from Python tracker
    if max_date is not None:
        most_recent_date = max_date
        max_date = None

    time.sleep(2)

if len(batch) > 0:
        df = spark.createDataFrame(batch)

        if not table_created:
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("country_id") \
                .save('Files/Bronze2/Sensors')
            table_created = True
        else:
            df.write \
                .format("delta") \
                .mode("append") \
                .save('Files/Bronze2/Sensors')

        print(f"Final batch written: {len(batch)} rows")
        batch = []


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(after_df.tail(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
