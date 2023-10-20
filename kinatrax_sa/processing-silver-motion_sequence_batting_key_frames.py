# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

# Imports have already been set in the 00_setup notebook
# Widgets and task values
# dbutils.widgets.text(name='table', defaultValue='motion_sequence', label='Table')

# Pipeline and task variables
TABLE = 'motion_sequence_batting_key_frames' #dbutils.widgets.get('table')
EVENT_TYPE = 'batting' #dbutils.widgets.get('event_type')
print(f"Table: {TABLE}")
print(f"Event Type: {EVENT_TYPE}")

# Path variables
table_silver = f'{CATALOG}.{DATABASE_S}.{EVENT_TYPE}_{TABLE}'
# CHECKPOINT_BASE = f'dbfs:/user/{CURRENT_USER}/{CATALOG}'
checkpoint_location_silver = f'{CHECKPOINT_BASE}/silver/batting_{TABLE}_checkpoint'
table_bronze_msb = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_motion_sequence_batting'
table_bronze_mtrp = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_motion_tracker_result_parameters'

# Configuration variables

print(f"Bronze Table 1: {table_bronze_msb}")
print(f"Bronze Table 2: {table_bronze_mtrp}")
print(f"Silver Table: {table_silver}")
print(f"Silver Checkpoint Location: {checkpoint_location_silver}")

# COMMAND ----------

# DDL configs
spark.sql(f"""CREATE CATALOG IF NOT EXISTS {CATALOG}""")
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE_S}""")
spark.sql(f"""SHOW DATABASES IN {CATALOG}""").display()

# COMMAND ----------

# Stream in the motion_sequence_batting table
df_motion = (
  spark.readStream
  .format('delta')
  .table(f'{table_bronze_msb}')
)

# COMMAND ----------

# Batch read the keyframes table to filter out key frames
df_keyframes = (
  spark.read
  .format('delta')
  .table(f'{table_bronze_mtrp}')
)

# COMMAND ----------

# Define a UDF to convert an array to a dictionary
def arr_to_map(arr):
    return {f'key_frame_{i}': value  for i, value in enumerate(arr)}
  
# Register the UDF
array_to_map_udf = udf(arr_to_map, MapType(StringType(), StringType()))

# COMMAND ----------

# Define the rounding function
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

# Explode and clean up the key frames table before the join
df_keyframes_exploded = (
  df_keyframes
  .withColumn('AreKeyFramesDetected', f.regexp_replace(f.col("AreKeyFramesDetected"), "[{}]", ""))
  .withColumn('KeyFrameIndices', f.regexp_replace(f.col("KeyFrameIndices"), "[{}]", ""))
  .withColumn('KeyFrameDetectionScores', f.regexp_replace(f.col("KeyFrameDetectionScores"), "[{}]", ""))
  .withColumn('AreKeyFramesDetected', f.split(f.col('AreKeyFramesDetected'), ';'))
  .withColumn('KeyFrameIndices', f.split(f.col('KeyFrameIndices'), ';'))
  .withColumn('KeyFrameDetectionScores', f.split(f.col('KeyFrameDetectionScores'), ';'))
  .withColumn('SubtractionFactor', f.col('KeyFrameIndices')[0])
  .withColumn('KeyFrameIndicesAdjusted', f.transform(f.col("KeyFrameIndices"), lambda x: (x - f.col("SubtractionFactor"))*3.333333))
  .select(f.posexplode(f.arrays_zip("AreKeyFramesDetected", "KeyFrameIndicesAdjusted", "KeyFrameDetectionScores")).alias("key_frame_position_name", "values"), "input_event", "input_file", "_rescued_data")
  .select("values.AreKeyFramesDetected", "values.KeyFrameIndicesAdjusted", "values.KeyFrameDetectionScores", "input_event", "input_file", "_rescued_data")
  .withColumn("KeyFrameIndicesAdjusted", round_udf(f.col("KeyFrameIndicesAdjusted").cast("double")))
  # Filtering out only clean records
  .where(f.col("_rescued_data").isNull())
  .withColumnRenamed("input_event", "input_event_kf")
  .drop(f.col("input_file"))
  .drop(f.col("_rescued_data"))
)
df_keyframes_exploded.display()

# COMMAND ----------

# Create row indices for the motion sequence table to filter out key frames
df_motion_ordered = (
  df_motion
  #.withColumn("row_index", f.row_number().over(Window.partitionBy("input_event").orderBy("Timestamp")))
  .withColumn("TimestampRounded", round_udf(f.col("Timestamp")))
  .drop(f.col("input_file"))
)

# COMMAND ----------

#df_motion_ordered.display()

# COMMAND ----------

# Left joining the key frames dataframe to the motion_sequence_batting dataframe to filter out key frame rows
joined_df = (
  df_keyframes_exploded.join(
    df_motion_ordered,
    (df_keyframes_exploded.input_event_kf == df_motion_ordered.input_event)
    & (df_keyframes_exploded.KeyFrameIndicesAdjusted == df_motion_ordered.TimestampRounded),
    'inner'
  )
  .drop("input_event_kf")
)

# COMMAND ----------

#joined_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The above code works with a batch DataFrame but fails with a stream since the window() operator only allows for windowing over a timestamp column.
# MAGIC
# MAGIC  - Hence we now join on timestamp rounded field so that we don't need to apply row_number function with a window on event_name on the motion_sequence_batting table
# MAGIC  - This resolves the above issue. The stream-batch join ensures that this can work in a stream-batch as well as continuous streaming fashion as long as the bronze tables have been updated before

# COMMAND ----------

(
  joined_df
  .writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_location_silver)
  .option("mergeSchema", True)
  .trigger(availableNow=True)
  .table(table_silver)
)

# COMMAND ----------

# Data validation checks
df_silver = (
  spark.read
  .format('delta')
  .table(table_silver)
)

print("Number of records in silver: ", df_silver.count())

# COMMAND ----------

df_silver.select("input_event").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: Clean up this notebook. Maybe join with batter-parameter-set table to add game metadata?
