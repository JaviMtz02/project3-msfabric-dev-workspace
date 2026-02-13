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

from pyspark.sql.functions import col, monotonically_increasing_id, explode, lit

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

# CELL ********************

first_run = True

for country_id in country_ids_list:
    if first_run:
        wr_mode = 'overwrite'
    else:
        wr_mode = 'append'

    location_df = spark.read.format('delta').load(f'Files/Bronze/Locations/country_id={country_id}')
    sensor_parent = spark.read.format('delta').load(f'Files/Bronze/Sensors/country_id={country_id}')

    sensor_parent = sensor_parent.withColumn('record_id', monotonically_increasing_id())

    # ============== Joined table (Setup) ==============
    join_setup = location_df.select(
        '*',
        explode("sensors").alias("m")
    ).withColumn("sensorids", col("m.id")).drop('m') \
     .withColumn('country_id', lit(country_id))

    joined = sensor_parent.join(join_setup, sensor_parent.sensor_id == join_setup.sensorids,'outer') \
                          .drop_duplicates()

    # ============== Location table (Partitioned) ==============
    lookup_location_df = joined.select('country_id', 'id','area','locality','timezone','bounds','country_name','country_code', 'coordinates_longitude', 'coordinates_latitude') \
                               .drop_duplicates()

    lookup_location_df.write.format('delta') \
                            .mode(wr_mode) \
                            .partitionBy('country_id') \
                            .saveAsTable('LocationTable')

    # ============== Owner table (Partitioned) ==============
    owner_lookup_df = joined.select('country_id', 'owner_id','owner_name') \
                            .drop_duplicates()

    owner_lookup_df.write.format('delta') \
                         .mode(wr_mode) \
                         .partitionBy('country_id') \
                         .saveAsTable('OwnerLookupTable')

    # ============== Provider table (Partitoned) ==============
    provider_lookup_df = joined.select('country_id', 'provider_id','provider_name') \
                               .drop_duplicates()

    provider_lookup_df.write.format('delta') \
                            .mode(wr_mode) \
                            .partitionBy('country_id') \
                            .saveAsTable('ProviderLookupTable')

    # ============== Pollutant table (Partitioned) ==============
    pollutant_lookup_df = joined.select('country_id', col('pollutant_abbrev').alias('pollutant_type_id'),'pollutant_name','units','units_description') \
                                .drop_duplicates() \
                                .dropna()

    pollutant_lookup_df.write.format('delta') \
                             .mode(wr_mode) \
                             .partitionBy('country_id') \
                             .saveAsTable('PollutantLookupTable')

    # ============== Parent table (Partitioned) ==============
    joined = joined.select(
                           'country_id',
                           'record_id',
                           'sensor_id',
                           'value', 
                           col('id').alias('location_id'), 
                           'provider_id', 
                           'owner_id', 
                           col('pollutant_abbrev').alias('pollutant_type_id'),
                           col('dtfUTC').alias('Uptime_Start'),
                           col('dtlUTC').alias('Uptime_End'),
                           col('start_local').alias('Reading_Start_Local'),
                           col('end_local').alias('Reading_End_Local'),
                           col('start_utc').alias('Reading_Start_UTC'),
                           col('end_utc').alias('Reading_End_UTC')
                           )


    joined.write.format('delta') \
                .mode(wr_mode) \
                .partitionBy('country_id') \
                .saveAsTable('Historical_ParentTable')
    
    first_run = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
