# Databricks notebook source
validate_all_data = True

# COMMAND ----------

# Imports
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string
from pyspark.sql.window import Window
from itertools import chain

# COMMAND ----------

# Widgets
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text(name="catalog", defaultValue="kinatrax", label="Catalog")

# COMMAND ----------

# Global variables
CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
CATALOG = dbutils.widgets.get("catalog")
DATABASE_B = 'bronze'
DATABASE_S = 'silver'
DATABASE_G = 'gold'
SCHEMA_BASE = f'dbfs:/user/{CURRENT_USER}/{CATALOG}'
CHECKPOINT_BASE = f'dbfs:/user/{CURRENT_USER}/{CATALOG}'

# COMMAND ----------

bronze_tables = list(spark.sql("""SHOW TABLES IN kinatrax.bronze""").select('tableName').toPandas()['tableName'].values)
silver_tables = list(spark.sql("""SHOW TABLES IN kinatrax.silver""").select('tableName').toPandas()['tableName'].values)

# COMMAND ----------

if validate_all_data:
  print("Bronze Tables:\n")
  for table in bronze_tables:
    print(f'Table: {table}')
    df = spark.read.table(f'kinatrax.bronze.{table}')
    print(f'Number of rows: {df.count()}')
    print(f'Number of unique events in table: {df.select("input_event").distinct().count()}')
    print('===========================================================')
  print("\nSilver Tables:\n")
  for table in silver_tables:
    print(f'Table: {table}')
    df = spark.read.table(f'kinatrax.silver.{table}')
    print(f'Number of rows: {df.count()}')
    print(f'Number of unique events in table: {df.select("input_event").distinct().count()}')
    print('===========================================================')

# COMMAND ----------

clear_all_data =  False

# /Volumes/kinatrax/landing/batting/game1/2023_04_26_19_10_28_Washington_Nationals_17_Alex_Call_Home/motion_sequence.csv
if clear_all_data:
  #spark.sql(f"""DROP DATABASE IF EXISTS {CATALOG}.{DATABASE_S} CASCADE""")
  #print("Deleted bronze database!")
  #spark.sql(f"""DROP TABLE IF EXISTS {CATALOG}.{DATABASE_B}.{EVENT_TYPE}_{TABLE}""")
  #print("Deleted bronze table!")
  dbutils.fs.rm(CHECKPOINT_BASE, True)
  dbutils.fs.rm(SCHEMA_BASE, True)
  print("Checkpoint and schema cleared!")

# COMMAND ----------

#from chispa.dataframe_comparer import assertSmallDataFrameEquality

# Assuming df1 and df2 are the two DataFrames you want to compare
#assert_df_equality(df1, df2)

# COMMAND ----------

# MAGIC %md ### Unit Test for zipWithIndex
# MAGIC
# MAGIC # Create a test PySpark DataFrame
# MAGIC from pyspark.sql import Row
# MAGIC test_data = [Row(row_index=10, input_event='event1', col1='a'), Row(row_index=20, input_event='event1', col1='b'), Row(row_index=30, input_event='event2', col1='c')]
# MAGIC test_df = spark.createDataFrame(test_data)
# MAGIC
# MAGIC # Define expected schema for output DataFrame
# MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# MAGIC expected_schema = StructType([StructField('row_index', IntegerType(), True),
# MAGIC                               StructField('input_event', StringType(), True),
# MAGIC                               StructField('col1', StringType(), True),
# MAGIC                               StructField('row_number', IntegerType(), True)])
# MAGIC
# MAGIC # Define expected output DataFrame
# MAGIC expected_data = [(10, 'event1', 'a', 1), (20, 'event1', 'b', 2), (30, 'event2', 'c', 1)]
# MAGIC expected_df = spark.createDataFrame(expected_data, expected_schema)
# MAGIC
# MAGIC # Define unit test function
# MAGIC def test_static_df_frames2():
# MAGIC   # Apply transformation to test data
# MAGIC   result_df = (
# MAGIC       test_df.rdd
# MAGIC       .zipWithIndex()
# MAGIC       .map(lambda row: row[0] + (row[1],))
# MAGIC       .toDF(expected_schema)
# MAGIC       .withColumn('row_number', f.row_number().over(Window.partitionBy('input_event').orderBy('row_index')))
# MAGIC   )
# MAGIC
# MAGIC   # Assert that the test and expected DataFrames are equal
# MAGIC   print(result_df.collect())
# MAGIC   print(expected_df.collect())
# MAGIC   assert result_df.collect() == expected_df.collect()
# MAGIC   print("TEST PASSED!")
# MAGIC
# MAGIC # Run the unit test
# MAGIC test_static_df_frames2()
