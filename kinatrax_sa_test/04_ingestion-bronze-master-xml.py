# Databricks notebook source
# MAGIC %run ./00_ddl

# COMMAND ----------

# MAGIC %run ./01_imports

# COMMAND ----------

spark.conf.set("spark.databricks.sql.nativeXmlDataSourcePreview.enabled", True)

# COMMAND ----------

# Imports have already been set in the 00_setup notebook
# Widgets and task values
dbutils.widgets.text(name='TABLE', defaultValue='', label='Table')
dbutils.widgets.text(name='EVENT_TYPE', defaultValue='', label='Event Type')
dbutils.widgets.text(name='ROOTTAG', defaultValue='', label='XML Root Tag')
dbutils.widgets.text(name='ROWTAG', defaultValue='', label='XML Row Tag')

# Pipeline and task variables
TABLE = dbutils.widgets.get('TABLE')
EVENT_TYPE = dbutils.widgets.get('EVENT_TYPE')
ROOTTAG = dbutils.widgets.get('ROOTTAG')
ROWTAG = dbutils.widgets.get('ROWTAG')
print(f"Table: {TABLE}")
print(f"Event Type: {EVENT_TYPE}") 
print(f"XML Root Tag: {ROOTTAG}")
print(f"XML Row Tag: {ROWTAG}")

# Path variables
table_bronze = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_{TABLE}'
schema_location_bronze = f'{SCHEMA_BASE}/{DATABASE_B}/{EVENT_TYPE}_{TABLE}_test_schema'
checkpoint_location_bronze = f'{CHECKPOINT_BASE}/{DATABASE_B}/{EVENT_TYPE}_{TABLE}_test_checkpoint'
base_path_input = f'dbfs:/Volumes/{CATALOG}/{DATABASE_L}/all_games_test/*/{EVENT_TYPE}/*'
file_path_input = f'{TABLE}.xml'

# Configuration variables

print(f"Bronze Table: {table_bronze}")
print(f"Bronze Schema Location: {schema_location_bronze}")
print(f"Bronze Checkpoint Location: {checkpoint_location_bronze}")
print(f"Landing Path: {base_path_input}/{file_path_input}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Code Description
# MAGIC The code below reads an XML file as stream from a directory base_path_input. It reads the XML file in the cloudFiles format and uses a schema defined in schema_location_bronze. It also specifies xml as the format of the input file. It uses the rootTag and rowTag options to identify the root and row tags of the XML file respectively. Additionally, it sets inferColumnTypes to True to automatically detect the data types based on the input data. Then it adds two new columns to the resulting DataFrame, input_file which contains the path of the input file and input_event using a UDF. The resulting DataFrame is stored in df.

# COMMAND ----------

# Ingest the dataset from the landing zone as an Autoloader stream
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "xml")
    .option("cloudFiles.schemaLocation", schema_location_bronze)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.inferColumnTypes", True)
    .option("rootTag", ROOTTAG)
    .option("rowTag", ROWTAG)
    .load(f'{base_path_input}/{file_path_input}')
    .withColumn("input_file", f.input_file_name())
    .withColumn("input_event", udf_extract_metadata("input_file", f.lit("input_event")))
    .withColumn("ingestion_timestamp", f.date_format(f.current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("string"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Code Description
# MAGIC The code below writes a stream of data stored in df to a Delta Lake table with the name table_bronze. It uses the Delta Lake streaming source by setting the format to delta. It specifies append as the output mode so that new records in the stream will be appended to the existing data in the Delta table. The .option() method sets the checkpoint location for this stream to checkpoint_location_bronze. Finally, the data is written to the Delta table using the table method. The Delta table must have been created before executing this code.

# COMMAND ----------

# Write the data to a bronze Delta table
(
  df
  .writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_location_bronze)
  .trigger(availableNow=True)
  .table(f"{table_bronze}_test")
)
