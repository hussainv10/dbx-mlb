# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

# Imports have already been set in the 00_setup notebook
# Widgets and task values
# dbutils.widgets.text(name='table', defaultValue='motion_sequence', label='Table')

# Pipeline and task variables
TABLE = 'track_joint_center_set_properties' #dbutils.widgets.get('table')
EVENT_TYPE = 'batting' #dbutils.widgets.get('event_type')
print(f"Table: {TABLE}")
print(f"Event Type: {EVENT_TYPE}")

# Path variables
table_bronze = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_{TABLE}'
# SCHEMA_BASE = f'dbfs:/user/{CURRENT_USER}/{CATALOG}'
# CHECKPOINT_BASE = f'dbfs:/user/{CURRENT_USER}/{CATALOG}'
schema_location_bronze = f'{SCHEMA_BASE}/bronze/batting_{TABLE}_schema'
checkpoint_location_bronze = f'{CHECKPOINT_BASE}/bronze/batting_{TABLE}_checkpoint'
# /Volumes/kinatrax/landing/all/game1/batting/2023_04_26_19_10_28_Washington_Nationals_17_Alex_Call_Home/motion_sequence_batting.csv
base_path_input = f'dbfs:/Volumes/{CATALOG}/landing/all/*/{EVENT_TYPE}'
file_path_input = f'*/{TABLE}.xml'
xml_schema_inference_file_path = f"{base_path_input}/{file_path_input}".replace("*/batting", "game1/batting")

# Configuration variables

print(f"Bronze Table: {table_bronze}")
print(f"Bronze Schema Location: {schema_location_bronze}")
print(f"Bronze Checkpoint Location: {checkpoint_location_bronze}")
print(f"Landing Path: {base_path_input}/{file_path_input}")
print(f"XML Schema Inference File Path: {xml_schema_inference_file_path}")

# COMMAND ----------

# DDL configs
spark.sql(f"""CREATE CATALOG IF NOT EXISTS {CATALOG}""")
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE_B}""")
spark.sql(f"""SHOW DATABASES IN {CATALOG}""").display()

# COMMAND ----------

# Extract the schema of the xml file
df_schema = (
  spark.read.format("binaryFile")
  .load(xml_schema_inference_file_path)
  .select(toStrUDF(f.expr("content")).alias("text"))
)

payloadSchema = (
  spark.read
  .format("com.databricks.spark.xml")
  .option("rootTag", "TrackJointCenterSetProperties")
  .option("rowTag", "TrackJointCenterSetProperties")
  .load(xml_schema_inference_file_path)
).schema

payloadSchema

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", schema_location_bronze)
    .option("cloudFiles.schemaEvolutionMode", "none")
    .option("cloudFiles.inferColumnTypes", True)
    .option("inferSchema", True)
    .option("mergeSchema", True)
    .load(f'{base_path_input}/{file_path_input}')
    .withColumn('text', toStrUDF(f.expr("content")))
    .withColumn('data', from_xml(f.expr("text"), payloadSchema))
    # .withColumn("input_file", f.input_file_name()) This doesn't work with the above configs
    .withColumnRenamed('path', 'input_file')
    .withColumn("input_event", udf_extract_metadata("input_file", f.lit("input_event")))
    .select("data", "input_file", "input_event")
)

# COMMAND ----------

(
  df
  .writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_location_bronze)
  .option("mergeSchema", True)
  .trigger(availableNow=True)
  .table(table_bronze)
)

# COMMAND ----------

# DBTITLE 1,Testing Continuous Trigger
# MAGIC %md
# MAGIC
# MAGIC (
# MAGIC   df
# MAGIC   .writeStream.format("delta")
# MAGIC   .outputMode("append")
# MAGIC   .option("checkpointLocation", checkpoint_location_bronze)
# MAGIC   .option("mergeSchema", True)
# MAGIC   .trigger(processingTime="30 seconds")
# MAGIC   .table(table_bronze)
# MAGIC )

# COMMAND ----------

# Data validation checks
df_bronze = (
  spark.read
  .format('delta')
  .table(table_bronze)
)

print("Number of records in bronze: ", df_bronze.count())

# COMMAND ----------

df_bronze.select("input_event").distinct().display()

# COMMAND ----------

df_bronze.display()

# COMMAND ----------

clear_all_data = False

if clear_all_data:
  #spark.sql(f"""DROP DATABASE IF EXISTS {CATALOG}.{DATABASE_B} CASCADE""")
  #print("Deleted bronze database!")
  spark.sql(f"""DROP TABLE IF EXISTS {CATALOG}.{DATABASE_B}.{EVENT_TYPE}_{TABLE}""")
  print("Deleted bronze table!")
  dbutils.fs.rm(checkpoint_location_bronze, True)
  dbutils.fs.rm(schema_location_bronze, True)
  print("Checkpoint and schema cleared!")

# COMMAND ----------


