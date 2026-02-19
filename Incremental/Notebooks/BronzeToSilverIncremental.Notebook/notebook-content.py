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
    substring,
    create_map,
    lower, 
    when
)

from pyspark.sql.types import StructType
from datetime import timedelta
from math import fabs

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

mw = {
    "co": 28.0106,
    "no2": 46.0055,
    "no": 30.006,
    "so2": 64.066,
    "o3": 47.998,
    "co2": 44.0095,
    "nox": 46.0055,
}

mw_map = create_map(*[x for k, v in mw.items() for x in (lit(k), lit(float(v)))])


for country_id in country_ids_list:
    location_df = spark.read.format('delta').load(f'Files/Bronze/Locations').filter(col('country_id') == country_id)
    sensor_parent = spark.read.format('delta').load(f'Files/BronzeIncremental/Sensors').filter((col('client_day') == target_day_only) & (col('country_id') == country_id))

    sensor_parent = sensor_parent.withColumn('record_id', monotonically_increasing_id()) \
                                .withColumn("mw", mw_map[col("pollutant_abbrev")]) \
                                .withColumn(
                                    "value",
                                    when(col("units") == "µg/m³", col("value").cast("double"))
                                    .when((col("units") == "ppb") & col("mw").isNotNull(),
                                        col("value").cast("double") * (col("mw") / lit(24.45)))
                                    .when((col("units") == "ppm") & col("mw").isNotNull(),
                                        col("value").cast("double") * (col("mw") / lit(24.45)) * lit(1000.0))
                                    .when(lower(col("units")) == "f",
                                        (col("value").cast("double") - lit(32.0)) * lit(5.0/9.0))
                                    .otherwise(col("value").cast("double"))
                                ) \
                                .withColumn(
                                "units",
                                when(col("units").isin("ppb", "ppm"), lit("µg/m³"))
                                .when(lower(col("units")) == "f", lit("c"))
                                .otherwise(col("units"))
                                ).drop(col('mw'))

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

# Validation

sensors_today = spark.read.format('delta') \
                          .load('Tables/incremental_parenttable') \
                          .filter(col('client_day') == target_day_only) \
                          .select('sensor_id') \
                          .distinct()

sensors_yesterday = spark.read.format('delta') \
                              .load(f'Tables/incremental_parenttable') \
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
