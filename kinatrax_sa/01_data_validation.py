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

# DDL configs
spark.sql(f"""CREATE CATALOG IF NOT EXISTS {CATALOG}""")
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE_B}""")
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE_S}""")
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE_G}""")
# spark.sql(f"""SHOW DATABASES IN {CATALOG}""").display()

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

if clear_all_data:
  spark.sql(f"""DROP DATABASE IF EXISTS {CATALOG}.{DATABASE_B} CASCADE""")
  print("Deleted bronze database!")
  #spark.sql(f"""DROP TABLE IF EXISTS {CATALOG}.{DATABASE_B}.{EVENT_TYPE}_{TABLE}""")
  #print("Deleted bronze table!")
  dbutils.fs.rm(CHECKPOINT_BASE, True)
  dbutils.fs.rm(SCHEMA_BASE, True)
  print("Checkpoint and schema cleared!")
