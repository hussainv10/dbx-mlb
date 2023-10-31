# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

# Imports have already been set in the 00_setup notebook

# Pipeline and task variables
TABLE = 'motion_sequence_batting_key_frames'
EVENT_TYPE = 'batting'
print(f"Table: {TABLE}")
print(f"Event Type: {EVENT_TYPE}")

# Path variables
table_silver = f'{CATALOG}.{DATABASE_S}.{EVENT_TYPE}_{TABLE}'
checkpoint_location_silver = f'{CHECKPOINT_BASE}/{DATABASE_S}/{EVENT_TYPE}_{TABLE}_checkpoint'
table_bronze_msb = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_motion_sequence_batting'
table_bronze_mtrp = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_motion_tracker_result_parameters'
table_bronze_bps = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_batter_parameter_set'

# Configuration variables
print(f"Bronze Table 1: {table_bronze_msb}")
print(f"Bronze Table 2: {table_bronze_mtrp}")
print(f"Bronze Table 3: {table_bronze_bps}")
print(f"Silver Table: {table_silver}")
print(f"Silver Checkpoint Location: {checkpoint_location_silver}")


# COMMAND ----------

# Stream in the motion_sequence_batting table
df_motion = (
  spark.readStream
  .format('delta')
  .table(f'{table_bronze_msb}')
)

# COMMAND ----------

# Stream in the keyframes table to filter out key frames
df_keyframes = (
  spark.readStream
  .format('delta')
  .table(f'{table_bronze_mtrp}')
)

# COMMAND ----------

# Batch read the batter_parameter_set bronze table to add metadata
df_batter_param_set = (
  spark.read
  .format('delta')
  .table(f'{table_bronze_bps}')
  # Filtering out only clean records
  .where(f.col("_rescued_data").isNull())
  .withColumnRenamed("input_event", "input_event_bps")
  .drop(f.col("_rescued_data"))
  .drop(f.col("input_file"))
)

# COMMAND ----------

# Define key frame names to enhance bronze table fields
key_frames_batting = ['StartBackLegWeightShift', 'MaximumBackLegWeightShift', 'MaximumLeadingLegLift', 'LeadingToeStrike', 'LeadingHeelStrike', 'BatElevation45Degrees', 'BatAzimuth0Degrees', 'BatAzimuth45Degrees', 'BallContact', 'BatAzimuth135Degrees', 'BatAzimuth180Degrees', 'BatAzimuth225Degrees', 'BatAzimuth270Degrees', 'BatAzimuth315Degrees', 'BatAzimuth360Degrees', 'LeadingForearmAzimuth315Degrees', 'EventEnd']

# Explode and clean up the key frames table before the join
df_keyframes_exploded = (
  df_keyframes
  .withColumn('AreKeyFramesDetected', f.regexp_replace(f.col("AreKeyFramesDetected"), "[{}]", ""))
  .withColumn('KeyFrameIndices', f.regexp_replace(f.col("KeyFrameIndices"), "[{}]", ""))
  .withColumn('KeyFrameDetectionScores', f.regexp_replace(f.col("KeyFrameDetectionScores"), "[{}]", ""))
  .withColumn('AreKeyFramesDetected', f.split(f.col('AreKeyFramesDetected'), ';'))
  .withColumn('KeyFrameIndices', f.split(f.col('KeyFrameIndices'), ';'))
  .withColumn('KeyFrameDetectionScores', f.split(f.col('KeyFrameDetectionScores'), ';'))
  .withColumn('KeyFrameNames', f.lit(key_frames_batting))
  .withColumn('SubtractionFactor', f.col('KeyFrameIndices')[0])
  .withColumn('KeyFrameIndicesAdjusted', f.transform(f.col("KeyFrameIndices"), lambda x: (x - f.col("SubtractionFactor"))*3.333333))
  .select(f.posexplode(f.arrays_zip("KeyFrameNames", "AreKeyFramesDetected", "KeyFrameIndicesAdjusted", "KeyFrameDetectionScores")).alias("key_frame_position_name", "values"), "input_event", "input_file", "_rescued_data")
  .select("values.KeyFrameNames", "values.AreKeyFramesDetected", "values.KeyFrameIndicesAdjusted", "values.KeyFrameDetectionScores", "input_event", "input_file", "_rescued_data")
  .withColumn("KeyFrameIndicesAdjusted", round_udf(f.col("KeyFrameIndicesAdjusted").cast("double")))
  # Filtering out only clean records
  .where(f.col("_rescued_data").isNull())
  .withColumnRenamed("input_event", "input_event_kf")
  .drop(f.col("row_index"))
  .drop(f.col("row_number"))
  .drop(f.col("_rescued_data"))
  .drop(f.col("input_file"))
)

# COMMAND ----------

# Create rounded timestamps for the motion sequence table to filter out key frames
df_motion_ordered = (
  df_motion
  .withColumn("TimestampRounded", round_udf(f.col("Timestamp")))
  # Filtering out only clean records
  .where(f.col("_rescued_data").isNull())
  .drop(f.col("row_index"))
  .drop(f.col("row_number"))
  .drop(f.col("_rescued_data"))
  .drop(f.col("input_file"))
)

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

# Now joining the result of this stream-stream join to batter_parameter_set to add game metadata
enriched_df = (
  joined_df.join(
    df_batter_param_set,
    (joined_df.input_event == df_batter_param_set.input_event_bps),
    'left'
  )
  .drop("input_event_bps")
)

# COMMAND ----------

# Write out the stream-stream-batch join to a silver table
(
  enriched_df
  .writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_location_silver)
  .option("mergeSchema", True)
  .trigger(availableNow=True)
  .table(table_silver)
)
