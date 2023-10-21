# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

# Imports have already been set in the 00_setup notebook
# Widgets and task values
dbutils.widgets.text(name='TABLE', defaultValue='', label='Table')
dbutils.widgets.text(name='EVENT_TYPE', defaultValue='', label='Event Type')
dbutils.widgets.text(name='DELIMITER', defaultValue=' ', label='Delimiter')

# Pipeline and task variables
TABLE = dbutils.widgets.get('TABLE')
EVENT_TYPE = dbutils.widgets.get('EVENT_TYPE')
DELIMITER = dbutils.widgets.get('DELIMITER')
print(f"Table: {TABLE}")
print(f"Event Type: {EVENT_TYPE}")
print(f"Delimiter: {DELIMITER}")

# Path variables
table_bronze = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_{TABLE}'
schema_location_bronze = f'{SCHEMA_BASE}/{DATABASE_B}/{EVENT_TYPE}_{TABLE}_schema'
checkpoint_location_bronze = f'{CHECKPOINT_BASE}/{DATABASE_B}/{EVENT_TYPE}_{TABLE}_checkpoint'
base_path_input = f'dbfs:/Volumes/{CATALOG}/landing/all/*/{EVENT_TYPE}'
file_path_input = f'*/{TABLE}.csv'

# Configuration variables

print(f"Bronze Table: {table_bronze}")
print(f"Bronze Schema Location: {schema_location_bronze}")
print(f"Bronze Checkpoint Location: {checkpoint_location_bronze}")
print(f"Landing Path: {base_path_input}/{file_path_input}")

# COMMAND ----------

# Ingest the dataset from the landing zone as an Autoloader stream
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location_bronze)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.inferColumnTypes", True)
    .option("inferSchema", True)
    .option("mergeSchema", True)
    .option("header", True)
    .option("delimiter", DELIMITER)
    .load(f'{base_path_input}/{file_path_input}')
    .withColumn("input_file", f.input_file_name())
    .withColumn("input_event", udf_extract_metadata("input_file", f.lit("input_event")))
)

# COMMAND ----------

# Write the data to a bronze Delta table
(
  df
  .writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_location_bronze)
  .option("mergeSchema", True)
  .trigger(availableNow=True)
  .table(table_bronze)
)
