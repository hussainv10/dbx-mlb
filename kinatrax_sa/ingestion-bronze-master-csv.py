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

# Extract the schema for the dataframe to be used in the zipWithIndex
schema_bronze = StructType([StructField("row_index", LongType(), True)] + df.schema.fields)

# COMMAND ----------

def add_row_index(df, batch_id):
  """
  Function to add row indices to motion_tracker_frame_result_parameters
  to facilitate an easy stream-stream join with the motion_sequence table

  # Write the transformed DataFrame to the database using foreachBatch
  .withColumn("row_number", f.row_number().over(Window.partitionBy('input_event').orderBy("Timestamp")))
  query = transformedDF.writeStream.foreachBatch(write_to_database).start()
  """
  # We don't have a column to order by, hence we first need to zipWithIndex and then Window
  df_zipped = (
    df.rdd.zipWithIndex().map(lambda row: (row[1],) + row[0]).toDF(schema_bronze)
    .withColumn('row_number', f.row_number().over(Window.partitionBy('input_event').orderBy("row_index")))
  )
  
  # Write the data to a bronze Delta table
  (
    df_zipped
    .write.format("delta")
    .mode("append")
    .option("mergeSchema", True)
    .saveAsTable(table_bronze)
  )

# COMMAND ----------

# Write the transformed DataFrame to the database using foreachBatch
(
  df
  .writeStream
  .foreachBatch(add_row_index)
  .option("checkpointLocation", checkpoint_location_bronze)
  .trigger(availableNow=True)
  .start()
)
