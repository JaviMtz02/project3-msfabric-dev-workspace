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

from pyspark.sql.functions import *
from pypinyin import pinyin, lazy_pinyin, Style
from pyspark.sql.types import ArrayType, StringType
import pykakasi
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, StringType,
    DoubleType, ArrayType
)


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

first_run = True

for country_id in country_ids_list:
    if first_run:
        wr_mode = 'overwrite'
        ov = 'false'
    else:
        wr_mode = 'append'
        ov = 'true'

    # Load tables filtered by country_id
    parenttable = spark.read.format('delta').load('Tables/historical_parenttable') \
        .filter(col('country_id') == country_id) \
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
    fact_table = parenttable.join(providertable, 'provider_id', 'inner') \
                            .select(
                                    col('parent_country_id').cast("string"),
                                    col('record_id').cast("string"),
                                    col('sensor_id').cast("string"),
                                    col('location_id').cast("string"),
                                    col('provider_id').cast("string"),
                                    col('provider_name').cast("string"),
                                    col('owner_id').cast("string"),
                                    col('pt_id').cast("string"),
                                    col('value').cast("double"),
                                    col('Reading_Start_UTC').cast("timestamp"),
                                    col('Reading_End_UTC').cast("timestamp")
                            ).dropna(subset=['record_id', 'value'], how='any')

    fact_table.write \
        .format('delta') \
        .mode(wr_mode) \
        .option('overwriteSchema', ov) \
        .partitionBy('parent_country_id') \
        .saveAsTable('fact_table')  # remove 'gold.' if Fabric

    # =========== dim_provider ===========
    dim_provider = providertable.select(
        col('provider_id').cast("string"),
        col('provider_name').cast("string"),
        col('country_id').cast("string")
    )

    dim_provider.write \
        .format('delta') \
        .mode(wr_mode) \
        .saveAsTable('dim_provider')

    # =========== dim_owner ===========
    dim_owner = ownertable.select(
        col('owner_id').cast("string"),
        col('owner_name').cast("string"),
        col('country_id').cast("string")
    )

    dim_owner.write \
        .format('delta') \
        .mode(wr_mode) \
        .saveAsTable('dim_owner')

    # =========== dim_pollutant ===========
    dim_pollutant = pollutanttable.select(
        col('pollutant_type_id').cast("string"),
        col('pollutant_name').cast("string"),
        col('units').cast("string"),
        col('country_id').cast("string")
    )

    dim_pollutant.write \
        .format('delta') \
        .mode(wr_mode) \
        .saveAsTable('dim_pollutant')

    # =========== dim_location ===========
    dim_location = locationtable.select(
        col('id').alias('location_id').cast('string'),
        col('area').cast("string"),
        col('locality').cast("string"),
        col('timezone').cast("string"),
        col('country_name').cast("string")
    )

    dim_location.write \
        .format('delta') \
        .mode(wr_mode) \
        .saveAsTable('dim_location')

    # =========== summary_area ===========
    summary_area = parenttable.join(locationtable, parenttable.location_id == locationtable.id, 'inner') \
        .select(
            col('sensor_id').cast("string"),
            col('location_id').cast("string"),
            col('country_code').cast("string"),
            col('Uptime_Start').cast("timestamp"),
            col('Uptime_End').cast("timestamp"),
            col('coordinates_longitude').alias('Longitude').cast("double"),
            col('coordinates_latitude').alias('Latitude').cast("double")
        ).dropDuplicates() \
         .dropna(subset=['sensor_id'])

    summary_area.write \
        .format('delta') \
        .mode(wr_mode) \
        .saveAsTable('summary_area')

    # =========== summary_pollutant_year ===========
    joined = parenttable.join(pollutanttable, parenttable.pt_id == pollutanttable.pollutant_type_id, 'inner') \
        .join(locationtable, locationtable.id == parenttable.location_id, 'inner') \
        .select(
            col('sensor_id').cast("string"),
            col('location_id').cast("string"),
            col('parent_country_id').cast("string"),
            col('Reading_Start_UTC').cast("timestamp"),
            col('Reading_End_UTC').cast("timestamp"),
            col('pt_id').cast("string"),
            col('value').cast("double"),
            col('area').cast("string"),
            col('locality').cast("string"),
            col('timezone').cast("string"),
            col('country_name').cast("string")
        ) \
        .withColumn('year', year(col('Reading_Start_UTC').cast('timestamp')).cast("int"))

    summary_pollutant_year = joined.join(pollutanttable, joined.pt_id == pollutanttable.pollutant_type_id, 'inner') \
        .select(
            col('country_name').cast("string"),
            col('area').cast("string"),
            col('locality').cast("string"),
            col('timezone').cast("string"),
            col('units').cast("string"),
            col('pollutant_name').cast("string"),
            col('year').cast("int"),
            col('value').cast("double"),
            col('pt_id').cast("string")
        ) \
        .groupBy('country_name', 'area', 'locality', 'timezone', 'year', 'units', 'pollutant_name', 'pt_id') \
        .agg(avg(col('value')).alias('value')) \
        .sort('year') \
        .withColumn(
            'Area',
            when(col('country_name') == 'China', lp_udf(col('area'))) \
            .when(col('country_name') == 'Japan', rm_udf(col('area'))) \
            .otherwise(col('area').cast("string"))
        )

    summary_pollutant_year.write \
        .format('delta') \
        .mode(wr_mode) \
        .saveAsTable('summary_pollutant_year')

    first_run = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
