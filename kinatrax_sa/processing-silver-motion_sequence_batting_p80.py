# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

# Imports have already been set in the 00_setup notebook
# Widgets and task values
# dbutils.widgets.text(name='table', defaultValue='motion_sequence', label='Table')

# Pipeline and task variables
TABLE = 'motion_sequence_batting_p80' #dbutils.widgets.get('table')
EVENT_TYPE = 'batting' #dbutils.widgets.get('event_type')
print(f"Table: {TABLE}")
print(f"Event Type: {EVENT_TYPE}")

# Path variables
table_silver = f'{CATALOG}.{DATABASE_S}.{EVENT_TYPE}_{TABLE}'
# CHECKPOINT_BASE = f'dbfs:/user/{CURRENT_USER}/{CATALOG}'
checkpoint_location_silver = f'{CHECKPOINT_BASE}/silver/batting_{TABLE}_checkpoint'
table_bronze_msb = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_motion_sequence_batting'
table_bronze_mtfrp = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_motion_tracker_frame_result_parameters'

# Configuration variables

print(f"Bronze Table 1: {table_bronze_msb}")
print(f"Bronze Table 2: {table_bronze_mtfrp}")
print(f"Silver Table: {table_silver}")
print(f"Silver Checkpoint Location: {checkpoint_location_silver}")

# COMMAND ----------

# DDL configs
spark.sql(f"""CREATE CATALOG IF NOT EXISTS {CATALOG}""")
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE_S}""")
spark.sql(f"""SHOW DATABASES IN {CATALOG}""").display()

# COMMAND ----------

# Define the schema for the mapping DataFrame
schema_mapping = StructType([
    StructField("motion_tracker_frame_result_parameters", StringType(), True),
    StructField("motion_sequence", StringType(), True),
    StructField("comments", StringType(), True)
])

# Define the data in arrays
motion_tracker_frame_result_parameters = [
    ("Hips = 0;", "Hips", None),
    ("Spine = 1;", "Spine", None),
    ("Neck = 2;", "Neck", None),
    ("HeadLeftSide = 3;", "Head", "The feature output in the motion_sequence file is the midpoint of HeadLeftSide and HeadRightSide."),
    ("HeadRightSide = 4;", "Head_dup", "The feature output in the motion_sequence file is the midpoint of HeadLeftSide and HeadRightSide."),
    ("LeftUpperArm = 5;", "LeftUpperArm", None),
    ("LeftForearm = 6;", "LeftForearm", None),
    ("LeftWrist = 7;", "LeftWrist", None),
    ("LeftHandLeftSidePalmUp = 8;", "LeftHand", "The feature output in the motion_sequence file is the midpoint of LeftHandLeftSidePalmUp and LeftHandRightSidePalmUp."),
    ("LeftHandRightSidePalmUp = 9;", "LeftHand_dup", "The feature output in the motion_sequence file is the midpoint of LeftHandLeftSidePalmUp and LeftHandRightSidePalmUp."),
    ("RightUpperArm = 10;", "RightUpperArm", None),
    ("RightForearm = 11;", "RightForearm", None),
    ("RightWrist = 12;", "RightWrist", None),
    ("RightHandLeftSidePalmUp = 13;", "RightHand", "The feature output in the motion_sequence file is the midpoint of RightHandLeftSidePalmUp and RightHandRightSidePalmUp."),
    ("RightHandRightSidePalmUp = 14;", "RightHand_dup", "The feature output in the motion_sequence file is the midpoint of RightHandLeftSidePalmUp and RightHandRightSidePalmUp."),
    ("LeftThigh = 15;", "LeftThigh", None),
    ("LeftShin = 16;", "LeftShin", None),
    ("LeftAnkle = 17;", "LeftAnkle", None),
    ("LeftFootLeftSide = 18;", "LeftFoot", "The feature output in the motion_sequence file is the midpoint of LeftFootLeftSide and LeftFootRightSide."),
    ("LeftFootRightSide = 19;", "LeftFoot_dup", "The feature output in the motion_sequence file is the midpoint of LeftFootLeftSide and LeftFootRightSide."),
    ("RightThigh = 20;", "RightThigh", None),
    ("RightShin = 21;", "RightShin", None),
    ("RightAnkle = 22;", "RightAnkle", None),
    ("RightFootLeftSide = 23;", "RightFoot", "The feature output in the motion_sequence file is the midpoint of RightFootLeftSide and RightFootRightSide."),
    ("RightFootRightSide = 24;", "RightFoot_dup", "The feature output in the motion_sequence file is the midpoint of RightFootLeftSide and RightFootRightSide.")
]

# Create a Spark DataFrame from the data in the arrays
df_mapping = spark.createDataFrame(motion_tracker_frame_result_parameters, schema_mapping)

# Convert all strings to lowercase in the DataFrame
df_mapping = df_mapping.select([f.lower(f.col(c)).alias(c) for c in df_mapping.columns]).toPandas()

# Display the DataFrame
df_mapping.display()

# COMMAND ----------

# Stream in the motion_sequence_batting table
df_motion = (
  spark.read
  .format('delta')
  .table(f'{table_bronze_msb}')
)

# COMMAND ----------

# Stream in the motion_tracker_frame_result_parameters table
df_frames = (
  spark.read
  .format('delta')
  .table(f'{table_bronze_mtfrp}')
)

# COMMAND ----------

df_motion.display()

# COMMAND ----------

df_frames.display()

# COMMAND ----------

df_frames2 = (
  df_frames
  # Add filter for where _rescued_data column is Null
  .withColumnRenamed(df_frames.columns[0], 'TrackingStates')
  .withColumnRenamed(df_frames.columns[1], 'ConfidenceValues')
  .withColumnRenamed(df_frames.columns[2], 'BoneFittingErrors')
  .withColumn("TrackingStates", f.regexp_replace("TrackingStates", "[{}]", ""))
  .withColumn("TrackingStates", f.split(f.col('TrackingStates'), ";"))
  .withColumn("ConfidenceValues", f.regexp_replace("ConfidenceValues", "[{}]", ""))
  .withColumn("ConfidenceValues", f.split(f.col('ConfidenceValues'), ";"))
  .withColumn("BoneFittingErrors", f.regexp_replace("BoneFittingErrors", "[{}]", ""))
  .withColumn("BoneFittingErrors", f.split(f.col('BoneFittingErrors'), ";"))
  .select("*", *[f.col("TrackingStates").getItem(i).alias(f'{df_mapping["motion_sequence"][i]}_TrackingStates') for i in range(df_mapping.shape[0])])
  .select("*", *[f.col("ConfidenceValues").getItem(i).alias(f'{df_mapping["motion_sequence"][i]}_ConfidenceValues') for i in range(df_mapping.shape[0])])
  .select("*", *[f.col("BoneFittingErrors").getItem(i).alias(f'{df_mapping["motion_sequence"][i]}_BoneFittingErrors') for i in range(df_mapping.shape[0])])
)
df_frames2.display()

# COMMAND ----------

cols_to_zip = [col.lower() for col in df_frames2.columns if "righthand" in col.lower() and "_dup" not in col.lower()]
cols_to_zip

# COMMAND ----------

df_frames3 = (
  df_frames2
  .withColumn("righthand_map", f.create_map(*list(chain(*[[f.lit(c), f.col(c)] for c in cols_to_zip]))))
)
df_frames3.display()

# COMMAND ----------

cols_to_zip_motion = [col.lower() for col in df_motion.columns if "righthand" in col.lower() and "_dup" not in col.lower()]
cols_to_zip_motion

# COMMAND ----------

df_motion2 = (
  df_motion
  .withColumn("righthand_map", f.create_map(*list(chain(*[[f.lit(c.split('_')[-1]), f.col(c)] for c in cols_to_zip_motion]))))
  .drop("input_file")
)
df_motion2.display()

# COMMAND ----------

df_frames3.count()

# COMMAND ----------

df_joined = (
  df_motion2.join(
    df_frames3,
    ['input_event'],
    'inner'
  )
)
df_joined.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TESTING THE ABOVE JOIN AS A BATCH FROM INPUT STREAMS
# MAGIC - We can't perform a stream-stream join as streaming queries don't support windowing on non-timestamp columns (we have to use a window function to generate a row_number over each event name)
# MAGIC - We therefore read both streams from their source bronze delta tables
# MAGIC - We then write both out as temp tables in UC (temp tables will always contain incremental data for the latest Trigger.AvailableNow)
# MAGIC - We then read both temp tables as batch tables, window over event and generate row number to give us join keys (input_event, row_number) and join them
# MAGIC - We finally write the results of the join out (append) to the silver table
# MAGIC - We clean up the temp tables to ensure they are empty for the next batch, but will only contain incremental data as they still use a checkpoint to determine what bronze table version they'd previously read
# MAGIC
# MAGIC CONSIDERATIONS:
# MAGIC - We definitely can't do a true stream-stream join as the two streams require the window described above to generate the join keys
# MAGIC - We also wouldn't be able to handle late-arriving data as we are not setting a watermark on any column
# MAGIC - Assuming the above stream-batch-join process is the only feasible method for us to use streams, can we convert the Trigger.AvaiableNow to a continuous one? (we can do this so far for all other bronze and silver processes)
# MAGIC - If we were not doing a join but just using a single stream at a time for which we required to use a window, we could use the foreachBatch method with the stream to apply a batch function that converts the batch to a pandas dataframe and performs computations on it, or maybe even just a regular row_index for the batch. Each batch here, however, would be all tha files pulled in by the current trigger, based on the configurations for the stream like maxFilesPerTrigger or maxBytesPerTrigger

# COMMAND ----------

# Streaming-Batch conversion for each batch of data being read
sdf_motion = (
  spark.readStream
  .format('delta')
  .table(f'{table_bronze_msb}')
)

# COMMAND ----------

sdf_frames = (
  spark.readStream
  .format('delta')
  .table(f'{table_bronze_mtfrp}')
)

# COMMAND ----------

static_motion = (
  sdf_motion
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_location_silver+"_tempstatic")
  .outputMode("append")
  .trigger(availableNow=True)
  .table("kinatrax.silver.static_motion_temp")
)

# COMMAND ----------

static_df_motion = spark.read.table("kinatrax.silver.static_motion_temp")
static_df_motion.count()

# COMMAND ----------

dbutils.fs.head("dbfs:/user/hussain.vahanvaty@databricks.com/kinatrax/silver/batting_motion_sequence_batting_p80_checkpoint_tempstatic/offsets/0")

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hussain.vahanvaty@databricks.com/kinatrax/silver/batting_motion_sequence_batting_p80_checkpoint_tempstatic/", True)
