# Databricks notebook source
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

def extract_metadata(file_path, entity='game'):
  """
  Extracts metadata from the given file_path based on the field required

  TODO: Can enhance this to use an LLM or NER model to avoid requiring custom rules
  """
  if entity == 'game':
    return file_path.split('/')[5]
  elif entity == 'event_type':
    return file_path.split('/')[6]
  elif entity == 'date':
    return ('-').join(file_path.split('/')[7].split('_')[0:3])
  elif entity == 'date_time':
    return ('-').join(file_path.split('/')[7].split('_')[0:6])
  elif entity == 'team':
    return (' ').join(file_path.split('/')[8].split('_')[6:8])
  elif entity == 'jersey_num':
    return file_path.split('/')[6].split('_')[9]
  elif entity == 'player_name':
    return (' ').join(file_path.split('/')[7].split('_')[9:11])
  elif entity == 'location':
    return file_path.split('/')[7].split('_')[-1]
  elif entity == 'input_event':
    return ('/'.join(file_path.split('/')[:-1]))
  else:
    raise Exception("Invalid Parameter!")
  
# Register the UDF with Spark
udf_extract_metadata = udf(extract_metadata, StringType())

# COMMAND ----------

#extract_metadata('dbfs:/Volumes/hussain_v/kinatrax_demo_landing/game1/Batting/2023_04_26_19_10_42_Washington_Nationals_17_Alex_Call_Home/batter_parameter_set.xml', 'input_event')

# COMMAND ----------

toStrUDF = udf(lambda bytes: bytes.decode('utf-8'))

# COMMAND ----------

def from_xml(col, schema, options={}):
  scala_datatype = spark._jsparkSession.parseDataType(schema.json())
  scala_options = sc._jvm.PythonUtils.toScalaMap(options)
  jc = sc._jvm.com.databricks.spark.xml.functions.from_xml(col._jc if isinstance(col, Column) else col, scala_datatype, scala_options)
  return Column(jc)

# COMMAND ----------

# Define the rounding function for motion_sequence timestamps for key frame joining
def round_func(n):
    if type(n) == type(None):
      return 0
    if (int(n) % 10 == 9):
        return int(n) + 1
    else:
        return int(n)

# Register the UDF
round_udf = udf(round_func, IntegerType())

# COMMAND ----------

validate_all_data = False

if validate_all_data:
  dbutils.widgets.text('DATABASE', 'bronze')
  DATABASE = dbutils.widgets.get('DATABASE')
  dbutils.widgets.text('TABLE', 'motion_sequence_batting')
  TABLE = dbutils.widgets.get('TABLE')
  dbutils.widgets.text('EVENT_TYPE', 'batting')
  EVENT_TYPE = dbutils.widgets.get('EVENT_TYPE')
  df = spark.read.table(f"{CATALOG}.{DATABASE}.{EVENT_TYPE}_{TABLE}")
  print("Number of records in table: ", df.count())
  df.select("input_event").distinct().display()

# COMMAND ----------

if validate_all_data:
  df.display()

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
