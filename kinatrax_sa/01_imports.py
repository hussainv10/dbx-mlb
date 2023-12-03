# Databricks notebook source
# Imports
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string
from pyspark.sql.window import Window
from itertools import chain

# COMMAND ----------

# MAGIC %md
# MAGIC Helper Functions used in the Kinatrax ETL process are defined below

# COMMAND ----------

def extract_metadata(file_path, entity='game'):
  """
  Extracts metadata from the given file_path based on the field required

  Args:
    file_path (str): The path of the file from which metadata needs to be extracted.
    entity (str, optional): The entity from which metadata needs to be extracted. Default is 'game'.

  Returns:
    str: The metadata value for the given entity.

  Raises:
    Exception: Invalid Parameter!, if the provided entity is not one of ['game', 'event_type', 'date', 'date_time', 'team', 'jersey_num', 'player_name', 'location', 'input_event'].
  """
  if entity == 'game':
    return file_path.split('/')[5]
  elif entity == 'event_type':
    return file_path.split('/')[4]
  elif entity == 'date':
    return ('-').join(file_path.split('/')[6].split('_')[0:3])
  elif entity == 'date_time':
    return ('-').join(file_path.split('/')[6].split('_')[0:6])
  elif entity == 'team':
    return (' ').join(file_path.split('/')[6].split('_')[6:8])
  elif entity == 'jersey_num':
    return file_path.split('/')[6].split('_')[8]
  elif entity == 'player_name':
    return (' ').join(file_path.split('/')[6].split('_')[9:11])
  elif entity == 'location':
    return file_path.split('/')[6].split('_')[-1]
  elif entity == 'input_event':
    return ('/'.join(file_path.split('/')[:-1]))
  else:
    raise Exception("Invalid Parameter!")
  
# Register the function as a UDF with Spark
udf_extract_metadata = udf(extract_metadata, StringType())

# COMMAND ----------

# toStrUDF defines a user-defined function (UDF) that takes a bytes object as input and decodes it into a utf-8 string using the decode() method.
toStrUDF = udf(lambda bytes: bytes.decode('utf-8'))

# COMMAND ----------

def from_xml(col, schema, options={}):
  """
  Parses a Spark `DataFrame` column containing xml data with the given schema according to the options.

  Args:
    col (Column): The DataFrame column containing the xml data to be parsed.
    schema (pyspark.sql.types.DataType): The xml schema to be used for parsing.
    options (dict, optional): The options that affect how the xml data is read.

  Returns:
    pyspark.sql.column.Column: The DataFrame column containing the parsed xml data.

  Notes:
    This function uses the 'from_xml' method from the databricks spark-xml library, which is a third-party library and thus must be installed beforehand.
  """
  scala_datatype = spark._jsparkSession.parseDataType(schema.json())
  scala_options = sc._jvm.PythonUtils.toScalaMap(options)
  jc = sc._jvm.com.databricks.spark.xml.functions.from_xml(col._jc if isinstance(col, Column) else col, scala_datatype, scala_options)
  return Column(jc)

# COMMAND ----------

# Define the rounding function for motion_sequence timestamps for key frame joining
def round_func(n):
    """
    Rounds the input value to the nearest 10th, unless the value is null, in which case 0 is returned.

    Args:
        n: A numeric value to be rounded.

    Returns:
        int: Returns the rounded value to the nearest 10th
    """
    if type(n) == type(None):
      return 0
    if (int(n) % 10 == 9):
        return int(n) + 1
    else:
        return int(n)

# Register the UDF
round_udf = udf(round_func, IntegerType())
