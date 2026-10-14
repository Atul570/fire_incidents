# Databricks notebook source
# MAGIC %run ./logger_class

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.types import StructType, StructField , IntegerType , StringType
from pyspark.sql.functions import current_timestamp
import requests
from datetime import datetime
from delta.tables import DeltaTable

logger = PipelineLogger("Bronze Fire Incidents Ingestion")



logger.info("Pipeline Configuration loaded" , api_url = API_URL , table_name = BRONZE_TABLE)

POINT_SCHEMA = StructType([
    StructField("type" , StringType() , True),
    StructField("coordinates" , StringType() , True)
])

SCHEMA= StructType([
    StructField("incident_number", StringType(), True),
    StructField("exposure_number", StringType(), True),
    StructField("id", StringType(), True),
    StructField("address", StringType(), True),

    # Keep date/time as StringType in Bronze
    StructField("incident_date", StringType(), True),
    StructField("call_number", StringType(), True),
    StructField("alarm_dttm", StringType(), True),
    StructField("arrival_dttm", StringType(), True),
    StructField("close_dttm", StringType(), True),

    StructField("city", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("battalion", StringType(), True),
    StructField("station_area", StringType(), True),
    StructField("box", StringType(), True),

    StructField("suppression_units", StringType(), True),
    StructField("suppression_personnel", StringType(), True),
    StructField("ems_units", StringType(), True),
    StructField("ems_personnel", StringType(), True),
    StructField("other_units", StringType(), True),
    StructField("other_personnel", StringType(), True),

    StructField("fire_fatalities", StringType(), True),
    StructField("fire_injuries", StringType(), True),
    StructField("civilian_fatalities", StringType(), True),
    StructField("civilian_injuries", StringType(), True),
    StructField("number_of_alarms", StringType(), True),

    StructField("primary_situation", StringType(), True),
    StructField("mutual_aid", StringType(), True),
    StructField("action_taken_primary", StringType(), True),
    StructField("property_use", StringType(), True),

    StructField("supervisor_district", StringType(), True),
    StructField("neighborhood_district", StringType(), True),

    # Reuse your existing point schema
    StructField("point", POINT_SCHEMA, True),

    StructField("data_as_of", StringType(), True),
    StructField("data_loaded_at", StringType(), True)
])

logger.info("Schema Defined" , total_field = len(SCHEMA.fields))


# COMMAND ----------

logger.step_start("API Data Fetch")

data = list()
table_exists_flag = spark.catalog.tableExists(BRONZE_TABLE )

logger.info("Table Existence Test",table_exists = table_exists_flag)

if not table_exists_flag:
    logger.info("FIRST LOAD_MODE : fetching all data from API" )
    limit = FIRST_LOAD_LIMIT
    offset = 0
    while True:
        params = {
        "$limit":limit,
        "$offset":offset
        }
        try:
            response = requests.get(API_URL,params=params)
            response.raise_for_status()

            results = response.json()
            data.extend(results)

            logger.info("API batch fetched",offset = offset , batch_size_len = len(results) , total_so_far = len(data))

            if len(results) < limit:
                logger.info("Reached end of API data")
                break
            offset+=limit
        except  requests.exceptions.RequestException as e:
            logger.error("API request failed" , error = str(e), offset = offset)
            break
    logger.success("FIRST load data fetched completed" , total_records = len(data))


else:
    logger.info("INCREMENTAL LOAD MODE: fetching new data only" , )

    limit =INCREMENTAL_LOAD_LIMIT 
    offset = 0
    last_date = spark.sql(f"""SELECT MAX(incident_date) AS LAST_DATE from {BRONZE_TABLE } """).collect()[0]["LAST_DATE"]

    if last_date is None:
        logger.warning("TABLE EXISTS :is empty or date not found")
        last_date ='1994-01-01'
        logger.info("SWITCHING TO FULL DATA INCREMENTAL LOAD SETTING DATE 1994-01-01")
    else:
        logger.info("incremental load started...")
    
    where = f"incident_date >= '{last_date}'" 
    while True:
        params = {
            "$limit" : limit,
            "$offset":offset,
            "$where":where
        }
        try:
            response = requests.get(API_URL,params=params)
            response.raise_for_status()

            results = response.json()
            data.extend(results)
            logger.info("API batch fetched",offset = offset , batch_size_len = len(results) , total_so_far = len(data))

            if len(results) < limit:
                logger.info("Reached end of API data")
                break
            offset+=limit
        except  requests.exceptions.RequestException as e:
            logger.error("API request failed" , error = str(e), offset = offset)
            break
    logger.success("INCREMENTAL load data fetched completed" , total_records = len(data))
logger.step_end("API Data Fetch")  # ← ADD THIS LINE


# COMMAND ----------

new_data_df = spark.createDataFrame(data , schema=SCHEMA)
new_data_df = new_data_df .withColumn("ingestion_datetime" , current_timestamp())
if len(data)== 0:
    logger.warning("NO DATA FOUND SKIPPING WRITING")
elif not table_exists_flag:
    logger.info(f'CREATING TABLE AND IT IS 1S DATA INGESTION INSERTING TOTAL RECORDS {len(data)}')
    new_data_df .write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(BRONZE_TABLE)
    total_records = spark.sql(f"SELECT COUNT(*) FROM {BRONZE_TABLE}").collect()[0][0]
    logger.success(f"SUCCESSFULLY INSERTED TOTAL_RECORDS: {total_records}")
elif new_data_df.count() > 0 :
    logger.info("STARTED INCREMENTAL LOAD OF NEW FETCHED DATA")
    delta_table = DeltaTable.forName(spark,BRONZE_TABLE)
    delta_table.alias("target").merge(
        new_data_df.alias("source"),
        """
        target.incident_number = source.incident_number
        AND target.exposure_number = source.exposure_number
        """
    ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
            .execute()
    logger.success("Data merged successfully", records_processed=len(data))

logger.step_end("Data Processing and Write")

# COMMAND ----------

logger.step_start("Data Quality Verification")

# 1. Final count
final_count = spark.table(BRONZE_TABLE).count()
logger.info("Final table count", total_records=f"{final_count:,}")

# 2. Check for duplicates
logger.info("Checking for duplicate records")
duplicates = spark.sql(f"""
    SELECT incident_number, exposure_number, COUNT(*) as cnt
    FROM {BRONZE_TABLE}
    GROUP BY incident_number, exposure_number
    HAVING cnt > 1
""")

duplicate_count = duplicates.count()
if duplicate_count > 0:
    logger.warning("Duplicates detected", duplicate_count=duplicate_count)
    print("\n🔍 Sample duplicates:")
    duplicates.show(5, truncate=False)
else:
    logger.success("No duplicates found")

# 3. Check for NULL primary keys
logger.info("Checking for NULL primary keys")
null_keys = spark.sql(f"""
    SELECT COUNT(*) as null_count
    FROM {BRONZE_TABLE}
    WHERE incident_number IS NULL OR exposure_number IS NULL
""").collect()[0]["null_count"]

if null_keys > 0:
    logger.warning("NULL primary keys detected", null_count=null_keys)
else:
    logger.success("All primary keys are valid")

# 4. Show sample of latest data
logger.info("Displaying sample of latest ingested data")
print("\n📋 Latest 5 records:")
spark.sql(f"""
    SELECT incident_number, incident_date, address, ingestion_datetime
    FROM {BRONZE_TABLE}
    ORDER BY ingestion_datetime DESC
    LIMIT 5
""").show(truncate=False)

# 5. Date range check
date_range = spark.sql(f"""
    SELECT 
        MIN(incident_date) as earliest_date,
        MAX(incident_date) as latest_date
    FROM {BRONZE_TABLE}
""").collect()[0]

logger.info("Data date range", 
           earliest=date_range["earliest_date"], 
           latest=date_range["latest_date"])

logger.step_end("Data Quality Verification")

# Pipeline summary
logger.pipeline_summary()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select primary_situation , count(*) from fire_incident.bronze.fire_incidents group by primary_situation order by primary_situation

# COMMAND ----------

