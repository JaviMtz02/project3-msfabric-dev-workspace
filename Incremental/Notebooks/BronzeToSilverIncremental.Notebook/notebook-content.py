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

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    lit, 
    explode, 
    monotonically_increasing_id,
    substring
)

from pyspark.sql.types import StructType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, monotonically_increasing_id

country_ids_list = [
    10, # China
    155, # USA
    9, # India 
    45, # Brazil 
    177, # Australia 
    190, # Japan  
    99, # Senegal
    37, # South Africa 
    50, # Germany
    192 # Iceland
]

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

wr_mode = 'overwrite'

for country_id in country_ids_list:
    location_df = spark.read.format('delta').load(f'Files/Bronze/Locations').filter(col('country_id') == country_id)
    sensor_parent = spark.read.format('delta').load(f'Files/BronzeIncremental/Sensors').filter((col('client_day') == target_day_only) & (col('country_id') == country_id))

    sensor_parent = sensor_parent.withColumn('record_id', monotonically_increasing_id())

    # ============== Joined table (Setup) ==============
    join_setup = location_df.select(
        '*',
        explode("sensors").alias("m")
    ).withColumn("sensorids", col("m.id")).drop('m') \

    joined = sensor_parent.join(join_setup, sensor_parent.sensor_id == join_setup.sensorids,'outer') \
                          .drop_duplicates()

    # ============== Parent table (Partitioned) ==============
    joined = joined.select(
                           sensor_parent["country_id"],
                           'record_id',
                           'sensor_id',
                           'value', 
                           col('id').alias('location_id'), 
                           'provider_id', 
                           'owner_id', 
                           col('pollutant_abbrev').alias('pollutant_type_id'),
                           col('dtfUTC').alias('Uptime_Start'),
                           col('dtlUTC').alias('Uptime_End'),
                           col('start_utc').alias('Reading_Start_UTC'),
                           col('end_utc').alias('Reading_End_UTC'),
                           col('start_local').alias('Reading_Start_Local'),
                           col('end_local').alias('Reading_End_Local'),
                           col('client_day')
                           )


    joined.write.format('delta') \
                .mode(wr_mode) \
                .partitionBy('client_day', 'country_id') \
                .saveAsTable("Incremental_ParentTable")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df = spark.read.format('delta').load('Tables/incremental_parenttable')

df = df.select(col('record_id'), col('Reading_Start_UTC'), col('Reading_Start_Local'))

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
