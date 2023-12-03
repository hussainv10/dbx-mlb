# Databricks notebook source
# MAGIC %run ./00_ddl

# COMMAND ----------

# MAGIC %run ./01_imports

# COMMAND ----------

# Imports have already been set in the 00_setup notebook
# Widgets and task values
dbutils.widgets.text(name='TABLE', defaultValue='', label='Table')
dbutils.widgets.text(name='EVENT_TYPE', defaultValue='', label='Event Type')

# Pipeline and task variables
TABLE = dbutils.widgets.get('TABLE')
EVENT_TYPE = dbutils.widgets.get('EVENT_TYPE')
print(f"Table: {TABLE}")
print(f"Event Type: {EVENT_TYPE}")

# Path variables
table_bronze = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_{TABLE}'
schema_location_bronze = f'{SCHEMA_BASE}/{DATABASE_B}/{EVENT_TYPE}_{TABLE}_schema'
checkpoint_location_bronze = f'{CHECKPOINT_BASE}/{DATABASE_B}/{EVENT_TYPE}_{TABLE}_checkpoint'
base_path_input = f'dbfs:/Volumes/{CATALOG}/{DATABASE_L}/all_games/*/{EVENT_TYPE}/*'
file_path_input = f'*{TABLE}.txt'

# Configuration variables

print(f"Bronze Table: {table_bronze}")
print(f"Bronze Schema Location: {schema_location_bronze}")
print(f"Bronze Checkpoint Location: {checkpoint_location_bronze}")
print(f"Landing Path: {base_path_input}/{file_path_input}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Code Description
# MAGIC The code below ingests a dataset using an Autoloader stream from a landing zone and loads it into a DataFrame called df.
# MAGIC
# MAGIC The DataFrame is created by reading data from a directory in the specified base_path_input, referencing a file called file_path_input in csv format. Note that file_path_input is a variable that must first have been initialized in the notebook.
# MAGIC
# MAGIC The load() function is used to carry out the reading operation.
# MAGIC
# MAGIC Options are set to specify the characteristics of the data in the dataset such as format, schema location, schema evolution mode, whether to infer column types and schema, whether to merge schema or not, header presence, delimiter used, as well as character manipulation options for the whitespace and line separator.
# MAGIC
# MAGIC Two columns are added to the DataFrame, input_file and input_event, using the withColumn() function. These two columns are created by calling respective UDF functions to extract metadata from the input file.

# COMMAND ----------

# Ingest the dataset from the landing zone as an Autoloader stream
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location_bronze)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.inferColumnTypes", False)
    .option("inferSchema", False)
    .option("mergeSchema", False)
    .option("header", True)
    .option("delimiter", '\t')
    .option("ignoreLeadingWhiteSpace", True)
    .option("ignoreTrailingWhiteSpace", True)
    .option("lineSep", "\n")
    .option("skipRows", 1)
    .load(f'{base_path_input}/{file_path_input}')
    .withColumn("input_file", f.input_file_name())
    .withColumn("input_event", udf_extract_metadata("input_file", f.lit("input_event")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Code Description
# MAGIC The code above writes data to a Delta table called table_bronze from a DataFrame called df.
# MAGIC
# MAGIC The writeStream() function is used on the DataFrame reference, and chained with other functions to enable streaming and define the output mode as "append".
# MAGIC
# MAGIC Options are set to specify the checkpoint location where stream progress is saved, whether to merge schema or not, and to make the stream immediately available for processing upon running the code block.
# MAGIC
# MAGIC Finally, table() function is called on the DataFrame reference to save the stream data to the Delta table.

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
