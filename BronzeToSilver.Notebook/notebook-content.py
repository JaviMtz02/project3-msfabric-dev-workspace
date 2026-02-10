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

from pyspark.sql.functions import col

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
    190, # Japan  
    99, # Senegal
    37, # South Africa 
    192 # Iceland  
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

col("id")
col("name").alias("area")
col("locality")
col("timezone")
col("instruments")
col("sensors")
col("bounds")
col("`datetimeFirst.utc`").alias("dtfUTC")
col("`datetimeLast.utc`").alias("dtlUTC")
col("`country.id`").alias("country_id")
col("`country.name`").alias("country_name")
col("`country.code`").alias("country_code")
col("`owner.id`").alias("owner_id")
col("`owner.name`").alias("owner_name")
col("`provider.id`").alias("provider_id")
col("`provider.name`").alias("provider_name")
col("`coordinates.longitude`").alias("coordinates_longitude")
col("`coordinates.latitude`").alias("coordinates_latitude")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for country_id in country_ids_list:
    country_df = spark.read.format('delta').load(f'Files/Bronze/Locations/country_id={country_id}').orderBy()

    sensor_table = country_df.select(col('sensors')) # Explode
    instrument_table = country_df.select(col('instruments')) # Explode
    provider_table = country_df.select(col('provider_id'), col('provider_name'))
    time_table = country_df.select(col('dtfUTC'), col('dtlUTC'), col('timezone'))
    bounds_table = country_df.select(col('bounds')) # Explode

    # <write>

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
OPTIMIZE 'Files/Bronze/Sensors/country_id='
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
DESCRIBE HISTORY delta.`Files/Bronze/Sensors`
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set(
    "spark.databricks.delta.optimize.maxFileSize",
    32 * 1024 * 1024  # 32 MB
)

spark.conf.set(
    "spark.databricks.delta.optimize.minFileSize",
    25 * 1024        # 25 KB
)

spark.sql("""
OPTIMIZE 'Files/Bronze/Sensors'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
