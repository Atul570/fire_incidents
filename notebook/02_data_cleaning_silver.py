# Databricks notebook source
# MAGIC %run ./logger_class

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

logger = PipelineLogger("Silver Fire Incidents Transformation")
logger.info("Silver layer pipeline initialized")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , TimestampType , StringType

logger.step_start('Watermark Table Existance check/creation')

watermark_schema = (
    StructType(
        [
            StructField('pipeline_name' , StringType() , False),
            StructField('last_processed_timestamp' , TimestampType() , False),
            StructField('updated_at' , TimestampType() , False)
        ]
    )
)

logger.info("Checking WaterMark Table")
if not spark.catalog.tableExists(WATERMARK_TABLE):
    logger.info("Watermark Table does not exists creating new")
    (
        spark.createDataFrame([] , watermark_schema)
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(WATERMARK_TABLE)
    )
    logger.success("watermark table created successfully")
else:
    logger.info("Watermark table already exists")
logger.step_end('Watermark Table Existance check/creation')

# COMMAND ----------

logger.info('checking last processed data in silver_fire_incident')

watermark_df = spark.sql(f'''
                                    SELECT last_processed_timestamp 
                                    FROM {WATERMARK_TABLE}
                                    WHERE pipeline_name = 'silver_fire_incidents'
                                     ''')
if watermark_df.count() == 0:
    logger.info("No information in watermark table for last processed - So processing all bronze data")
    last_processed = '1994-01-01 00:00:00'
    first_run = True
else:
    logger.info("last processed data " ,  last_processed_data = watermark_df.collect()[0]['last_processed_timestamp'])
    last_processed = watermark_df.collect()[0]['last_processed_timestamp']

df = spark.sql(f'''
               select * from {BRONZE_TABLE} WHERE ingestion_datetime > '{last_processed}' ''')
new_record_count = df.count()
if new_record_count == 0:
    logger.info("No New records to process - existing")
    dbutils.notebook.exit("No new data to procss")
else:
    logger.info("New Bronze Data Loaded for Processing",Total_record_to_process = df.count())
df_count = df.count()

# COMMAND ----------

logger.step_start("NULL ANALYSIS")
null_counts_row = df.select(
    [
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]
)

logger.info("NULL Analysis Completed")
null_counts_row.show()

for columns in df.columns:
    null_count = null_counts_row.select(columns).collect()[0][0]
    null_percentage = (null_count/df_count)*100

    if null_percentage > 5:
        logger.warning(f"High NULL rate in column: {columns}", 
                      null_count=f"{null_count:,}", 
                      null_percentage=f"{null_percentage:.1f}%")
logger.step_end("NULL ANALYSIS")


# COMMAND ----------

'''null_count = null_counts_row.selectExpr(
    "stack(" + 
    str(len(null_counts_row.columns)) +
    "," + 
    ",".join([f"'{c}' , `{c}`" for c in null_counts_row.columns]) + 
    ") as (column_name , null_count)"
)

null_count = null_counts_row.selectExpr(
    "stack(" + 
    str(len(null_counts_row.columns)) +
    "," + 
    ",".join([f"'{c}' , `{c}`" for c in null_counts_row.columns]) + 
    ") as (column_name , null_count)"
)

total_count = df.count()
null_count.select(
    
        "column_name" ,
        "null_count",
       round(
           (col("null_count")/lit(total_count))*100,
           2
       ).alias("Percentage")
).filter(col("null_count") > 0) .show()'''

# COMMAND ----------

logger.step_start("Clean Situation Codes")

extracted_code = F.regexp_extract(
            F.col("primary_situation"),
            r"^\s*(\d{3})",
            1
        )
df = df.withColumn(
    "situation_code",
    F.when((extracted_code != "") & (extracted_code.isNotNull()) , extracted_code)
).withColumn(
    "situation_text_raw",
    F.initcap(F.trim(F.regexp_replace(F.col("primary_situation") , r'^\S*\d+\s*-?\s*',"")))
)
logger.info("Creating situation_code and situation_text_raw columns from primary_situation")

logger.info("CREATING NEW DATA FRAME THAT WE USE FOR OUR LOOKUP")
df_lookup = (
    df
                .filter(F.col("situation_code").isNotNull())
                .groupBy(["situation_code" , "situation_text_raw"])
                .agg(F.count("*").alias("frequency"))
                .withColumn(
                    "rank",
                    F.row_number().over( Window.partitionBy("situation_code")
                                            .orderBy(F.col("frequency").desc())
                                )
                )
                
        ).filter(F.col("rank")== 1) \
        .select(
                    F.col("situation_code"),
                    F.col("situation_text_raw").alias("situation_text_standard")
                )

logger.info("WE GETTING ONLY THOSE TEXT WHICH MOST USE BECAUSE THERE MULTIPLE TEXT WITH LITTLE CHANGE ASSOCIATED WITH ONE CODE")
(
    df_lookup
        .write
        .mode('overwrite')
        .format('delta')
        .saveAsTable(SILVER_DIM_SITUATION_TABLE)
)



situation_lookup = spark.table(SILVER_DIM_SITUATION_TABLE)
lookup_count = situation_lookup.count()
logger.success("Situation lookup table created", 
              table=SILVER_DIM_SITUATION_TABLE,
              unique_codes=f"{lookup_count:,}")
logger.info("joining df with situation_code and getting clean situation text")

df = df.join(
    situation_lookup.select("situation_code" , "situation_text_standard"),
    on = "situation_code",
    how = "left"
).withColumnsRenamed(
    {
        "situation_text_standard": "situation_text",
        "primary_situation": "primary_situation_raw"
    }
).drop("situation_text_raw")


records_with_standard_text = df.filter(F.col("situation_text").isNotNull()).count()
logger.info("Situation text standardization completed",
           records_standardized=f"{records_with_standard_text:,}",
           join_type="left")
logger.step_end("Clean Situation Codes")

# COMMAND ----------

logger.step_start("City Clean")
df = df.withColumn(
    "city",
    F.when(F.upper(F.trim(F.col("city"))).isin("SF", "SAN FRANCISCO"), "San Francisco")
    .when(F.col("city") == "SFO", "San Francisco Airport")
    .when(F.upper(F.trim(F.col("city"))).isin("TI", "TREASURE ISLA", "TREASURE ISLAND"), "Treasure Island")
    .when(F.upper(F.trim(F.col("city"))).isin("PR", "PRESIDIO"), "Presidio")
    .when(F.col("city") == "YB", "Yerba Buena")
    .when(F.col("city") == "HP", "Hunters Point")
    .when(F.upper(F.trim(F.col("city"))).isin("FM", "FORT MASON"), "Fort Mason")
    .when(F.col("city") == "DC", "Daly City")
    .when(F.col("city") == "BN", "Brisbane")
    .when(F.col("city") == "OAK", "Oakland")
    .when(F.col("city") == "AI", "Alcatraz Island")
    .otherwise(F.trim(F.col("city")))
)
logger.step_end("City Clean")

# COMMAND ----------

logger.step_start("Cleaning ZIPCODE")
count = df.select("zipcode").distinct().count()
logger.info("TOTAL Distinct Zipcode before cleaning" , total_distinct_zipcode =count )
df = df.withColumn(
    "zipcode_clean",
    F.when(
        F.col("zipcode").rlike(r"^94\d{3}"),
        F.regexp_extract(F.col("zipcode") , r'^(94\d{3})',1)
    ).otherwise(None)
).drop("zipcode").withColumnRenamed("zipcode_clean","zipcode")
count = df.select("zipcode").distinct().count()
logger.info("TOTAL Distinct Zipcode After cleaning" , total_distinct_zipcode =count )
logger.step_end("Cleaning ZIPCODE")


# COMMAND ----------

logger.step_start("Cleaning City Columns")

null_count = df.select(F.sum(F.col("city").isNull().cast('int')).alias("null_count")).collect()[0][0]
logger.info(f"Total NULL value in city before cleaning : {null_count}")

total_freq= (df
    .filter(F.col("zipcode").isNotNull())
    .groupBy("zipcode")
    .agg(F.count("*").alias("total_count"))
    .orderBy(F.col("total_count").desc())
)

city_freq = (
    df
        .filter((F.col("zipcode").isNotNull()) & (F.col("city").isNotNull()))
        .groupBy("zipcode","city")
        .agg(F.count("*").alias("city_count"))
)

join_table = total_freq.join(
    city_freq,
    on = "zipcode",
    how= "inner"
)

city_lookup = (
        join_table.withColumn(
            "percentage",
            F.round((F.col("city_count") / F.col("total_count"))*100,2)
        ) 
    .orderBy(
        F.col("city_count").desc(),
        F.col("zipcode" ), 
        F.col("percentage").desc()
    ) 
    .withColumn(
        "rank",
        F.row_number().over(
            Window.partitionBy(F.col("zipcode")).
            orderBy(F.col("percentage").desc())
            )
    )
    .filter(F.col("rank")==1)
    .select("zipcode","city")
)

(
    city_lookup
        .write
        .format('delta')
        .mode("overwrite")
        .saveAsTable(SILVER_DIM_ZIPCODE_TABLE)
)


from pyspark.sql.functions import coalesce, col


zipcode_city_mapping = (
    spark.table(SILVER_DIM_ZIPCODE_TABLE)
    .select(
        F.col("zipcode").alias("lookup_zipcode"),
        F.col("city").alias("lookup_city")
    )
)


df = (
    df.join(
        zipcode_city_mapping,
        df.zipcode == F.col("lookup_zipcode"),
        how="left"
    )
    .withColumn(
        "city_new",
        F.coalesce(df.city, F.col("lookup_city"))
    )
    .drop("city", "lookup_zipcode", "lookup_city")  
    .withColumnRenamed("city_new", "city") 
)

null_count = df.select(F.sum(F.col("city").isNull().cast('int')).alias("null_count")).collect()[0][0]
logger.info(f"Total NULL value in city after cleaning : {null_count}")
logger.step_end("Cleaning City Columns")

# COMMAND ----------

logger.step_start("Cleaning Action Code")
#Cleaning action_taken_primay column by creating a lookup table which contain action code and most used action text for that that code , i am doing this because one code have multiple simlar text with the same code
extracted_code = F.regexp_extract(
    F.col("action_taken_primary"),
    r'^\s*(\d{2})',
    1
)
df = (
    df
        .withColumn(
            "action_code",
            F.when((extracted_code != "") & (extracted_code.isNotNull()) , extracted_code)
                .otherwise(None)
        )
        .withColumn(
            "action_taken_text_raw",
            F.when(F.col("action_code").isNotNull(),
            F.initcap(F.trim(F.regexp_replace(
                F.col("action_taken_primary"),
                r'^\s*\d*\s*-?\s*',
                ""
            )))).otherwise(None)
        )
)

lookup_df = (
    df
        .filter(col("action_code").isNotNull())
        .groupBy(["action_code" ,"action_taken_text_raw"])
        .agg(F.count("*").alias("frequency"))
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("action_code")
                    .orderBy(col("frequency").desc())
            )
        )
        .filter(F.col("rank") ==1 )
        .select("action_code" ,col("action_taken_text_raw").alias("action_taken_standard"),)
)

(
    lookup_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_DIM_ACTION_TABLE)
)

action_df = spark.table(SILVER_DIM_ACTION_TABLE)

df = df.join(
    action_df,
    on = "action_code",
    how = "left"
).withColumnsRenamed(
    {
        "action_taken_standard" : "action_text",
        "action_taken_primary" : "action_taken_raw"
    }
).drop("action_taken_text_raw")
logger.step_end("Cleaning Action Code")

# COMMAND ----------

logger.step_start("Creating Longitude and latitude Columns")
#We have a column point which contain json type text which contain latitude and longitude i am extracting herre using regular expression
df = df.withColumn(
    "longitude",
    F.regexp_extract(col("point.coordinates"),r'^\[([-\d.]+),',1).cast("double")
).withColumn(
    "latitude",
    F.regexp_extract(col("point.coordinates"),r',\s*([-\d.]+)\]',1).cast("double")
)

df.select("point", "longitude", "latitude").show(10, truncate=False)
logger.step_end("Creating Longitude and latitude Columns")

# COMMAND ----------

logger.step_start("Data Quality Check")

logger.info(f"Total Records {df.count()}")

dup_by_incident = df.groupBy("incident_number").count().filter(F.col("count") > 1).count()
logger.info("Duplicate ", count_duplicate_by_incident = dup_by_incident)


dup_by_both = df.groupBy("incident_number", "exposure_number").count().filter(F.col("count") > 1).count()
logger.info("Duplicate ", count_duplicate_by_incident_and_exposure = dup_by_both)


window = Window.partitionBy("incident_number", "exposure_number") \
               .orderBy(col("data_loaded_at").desc())

df = (
    df.withColumn("row_num", F.row_number().over(window))
    .filter(col("row_num") == 1)
    .drop("row_num")
)


logger.info(f"Total Count after Removing Duplicate : {df.count()} ")


dup_check = df.groupBy("incident_number", "exposure_number").count().filter(col("count") > 1).count()
logger.info("Remaining Duplicate ", count_duplicate = dup_check)
logger.step_end("Data Quality Check")

# COMMAND ----------

# After Cell 10 (geocoding), add this:

from pyspark.sql.functions import to_timestamp, col

df = df.select(
    F.col("incident_number").cast("int"),
    F.col("exposure_number").cast("int"),
    F.col("id").cast("int"),
    F.col("address"),
    F.to_timestamp(col("incident_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("incident_date"),
    F.col("call_number").cast("int"),
    F.to_timestamp(col("alarm_dttm"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("alarm_dttm"),
    F.to_timestamp(col("arrival_dttm"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("arrival_dttm"),
    F.to_timestamp(col("close_dttm"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("close_dttm"),
    F.col("city"),
    F.col("zipcode"),
    F.col("battalion"),
    F.col("station_area"),
    F.col("box"),
    F.col("suppression_units").cast("int"),
    F.col("suppression_personnel").cast("int"),
    F.col("ems_units").cast("int"),
    F.col("ems_personnel").cast("int"),
    F.col("other_units").cast("int"),
    F.col("other_personnel").cast("int"),
    F.col("fire_fatalities").cast("int"),
    F.col("fire_injuries").cast("int"),
    F.col("civilian_fatalities").cast("int"),
    F.col("civilian_injuries").cast("int"),
    F.col("number_of_alarms").cast("int"),
    F.col("situation_code"),
    F.col("situation_text"),
    F.col("primary_situation_raw"),
    F.col("mutual_aid"),
    F.col("action_code"),
    F.col("action_text"),
    F.col("action_taken_raw"),
    F.col("property_use"),
    F.col("supervisor_district").cast("int"),
    F.col("neighborhood_district"),
    F.col("longitude"),
    F.col("latitude"),
    F.to_timestamp(col("data_as_of"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("data_as_of"),
    F.to_timestamp(col("data_loaded_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("data_loaded_at")
)

# COMMAND ----------

logger.step_start("NULL check")



df.select(
    F.count(F.when(F.col("incident_number").isNull(), 1)).alias("incident_number_nulls"),
    F.count(F.when(F.col("incident_date").isNull(), 1)).alias("incident_date_nulls"),
    F.count(F.when(F.col("situation_code").isNull(), 1)).alias("situation_code_nulls"),
    F.count(F.when(F.col("action_code").isNull(), 1)).alias("action_code_nulls"),
    F.count(F.when(F.col("city").isNull(), 1)).alias("city_nulls"),
    F.count(F.when(F.col("latitude").isNull(), 1)).alias("latitude_nulls")
).show()






logger.info(f"Total columns: {len(df.columns)}")
logger.step_end("NULL check")

# COMMAND ----------

from delta.tables import DeltaTable

logger.step_start('Saving Processed Data')

if not spark.catalog.tableExists(SILVER_TABLE):
    logger.info('Table Does not Exists It is First Run')

    (
        df.write
        .format("delta")
        .mode("overwrite")  
        .option("overwriteSchema", "true") 
        .saveAsTable(SILVER_TABLE)
    )
else:
    logger.info('Table  Exists Only Saveing New Arrival Data ')
    delta_table= DeltaTable.forName(spark,SILVER_TABLE)
    delta_table.alias('target').merge(
        df.alias('source'),
        '''
            target.incident_number = source.incident_number
            AND target.exposure_number = source.exposure_number
        '''
    ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
            .execute()


logger.success("Data Written in Silver table Successfully")
logger.step_end('Saving Processed Data')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
logger.step_start('Watermark Update')
delta_table = DeltaTable.forName(spark,WATERMARK_TABLE)

processed_till = spark.sql(f'''
    SELECT MAX(ingestion_datetime) as max_ts 
    FROM {BRONZE_TABLE}
''').collect()[0]["max_ts"]  # ← Use column alias

logger.info("Latest Bronze timestamp", max_timestamp=str(processed_till))

# Create watermark update record
watermark_update = spark.createDataFrame([
    ("silver_fire_incidents", processed_till, datetime.now())
], ["pipeline_name", "last_processed_timestamp", "updated_at"])

delta_table.alias('target').merge(
    watermark_update.alias('source'),
    'target.pipeline_name = source.pipeline_name'
).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
            .execute()
logger.step_end('Watermark Update')




# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from fire_incident.silver.processing_watermark;

# COMMAND ----------

