# Databricks notebook source
# MAGIC %run ./00_ddl

# COMMAND ----------

# MAGIC %run ./01_imports

# COMMAND ----------

# Imports have already been set in the 00_setup notebook

# Pipeline and task variables
TABLE = 'motion_sequence_batting_master'
EVENT_TYPE = 'batting'
print(f"Table: {TABLE}")
print(f"Event Type: {EVENT_TYPE}")

# Path variables
table_silver = f'{CATALOG}.{DATABASE_S}.test_{EVENT_TYPE}_{TABLE}'
checkpoint_location_silver = f'{CHECKPOINT_BASE}/{DATABASE_S}/{EVENT_TYPE}_{TABLE}_test_checkpoint'
table_bronze_msb = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_motion_sequence_batting_test'
table_bronze_mtfrp = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_motion_tracker_frame_result_parameters_test'
table_bronze_bps = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_batter_parameter_set_test'

# Configuration variables
print(f"Bronze Table 1: {table_bronze_msb}")
print(f"Bronze Table 2: {table_bronze_mtfrp}")
print(f"Bronze Table 3: {table_bronze_bps}")
print(f"Silver Table: {table_silver}")
print(f"Silver Checkpoint Location: {checkpoint_location_silver}")

# COMMAND ----------

#dbutils.fs.rm(checkpoint_location_silver, recurse=True)

# COMMAND ----------

# Streaming in the motion_sequence_batting bronze table
df_motion = None

# Define the function to process each micro-batch
def stream_to_batch_ms(batch_df, batch_id):
    global df_motion
    if df_motion is None:
        df_motion = batch_df
    else:
        df_motion = df_motion.union(batch_df)

df_motion_stream = (
  spark.readStream
  .format('delta')
  .table(f'{table_bronze_msb}')
  # Filtering out only clean records
  .where(f.col("_rescued_data").isNull())
  .drop(f.col("_rescued_data"))
  .drop(f.col("input_file"))
)

df_motion_query = (
  df_motion_stream
  .writeStream
  .option('checkpointLocation', checkpoint_location_silver+"/motion")
  .foreachBatch(stream_to_batch_ms)
  .trigger(availableNow=True)
  .start()
)

# COMMAND ----------

df_motion.count()

# COMMAND ----------

df_motion.display()

# COMMAND ----------

# Streaming in the motion_sequence_batting bronze table
df_frames= None

# Define the function to process each micro-batch
def stream_to_batch_mtfrp(batch_df, batch_id):
    global df_frames
    if df_frames is None:
        df_frames = batch_df
    else:
        df_frames = df_frames.union(batch_df)

# Streaming in the motion_tracker_frame_result_parameters bronze table
df_frames_stream = (
  spark.readStream
  .format('delta')
  .table(f'{table_bronze_mtfrp}')
  # Filtering out only clean records
  .where(f.col("_rescued_data").isNull())
  .drop(f.col("_rescued_data"))
  .drop(f.col("input_file"))
  .drop(f.col("ingestion_timestamp"))
)

df_frames_query = (
  df_frames_stream
  .writeStream
  .option('checkpointLocation', checkpoint_location_silver+"/frames")
  .foreachBatch(stream_to_batch_mtfrp)
  .trigger(availableNow=True)
  .start()
)

# COMMAND ----------

df_frames.count()

# COMMAND ----------

df_frames.display()

# COMMAND ----------

# Batch read the batter_parameter_set bronze table to add metadata
df_batter_param_set = (
  spark.read
  .format('delta')
  .table(f'{table_bronze_bps}')
  .withColumnRenamed("input_event", "input_event_bps")
  # Filtering out only clean records
  .where(f.col("_rescued_data").isNull())
  .drop(f.col("_rescued_data"))
  .drop(f.col("input_file"))
  .drop(f.col("ingestion_timestamp"))
)

# COMMAND ----------

# Define the schema for the mapping DataFrame to define relationships between joint names in the motion_sequence and motion_tracker_frame_result_parameters files
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

# COMMAND ----------

# Transforming the mtfrp dataframe by extracting nested items
df_frames_processed = (
  df_frames
  # Add filter for where _rescued_data column is Null
  .withColumnRenamed(df_frames.columns[1], 'TrackingStates')
  .withColumnRenamed(df_frames.columns[2], 'ConfidenceValues')
  .withColumnRenamed(df_frames.columns[3], 'BoneFittingErrors')
  .withColumn("TrackingStates", f.regexp_replace("TrackingStates", "[{}]", ""))
  .withColumn("TrackingStates", f.split(f.col('TrackingStates'), ";"))
  .withColumn("TrackingStates", f.expr("transform(TrackingStates, x -> int(CASE WHEN x = 'T' THEN 1 ELSE 0 END))"))
  .withColumn("ConfidenceValues", f.regexp_replace("ConfidenceValues", "[{}]", ""))
  .withColumn("ConfidenceValues", f.split(f.col('ConfidenceValues'), ";").cast('array<double>'))
  .withColumn("BoneFittingErrors", f.regexp_replace("BoneFittingErrors", "[{}]", ""))
  .withColumn("BoneFittingErrors", f.split(f.col('BoneFittingErrors'), ";").cast('array<double>'))
  .select("*", *[f.col("TrackingStates").getItem(i).alias(f'{df_mapping["motion_sequence"][i]}_TrackingStates') for i in range(df_mapping.shape[0])])
  .select("*", *[f.col("ConfidenceValues").getItem(i).alias(f'{df_mapping["motion_sequence"][i]}_ConfidenceValues') for i in range(df_mapping.shape[0])])
  .select("*", *[f.col("BoneFittingErrors").getItem(i).alias(f'{df_mapping["motion_sequence"][i]}_BoneFittingErrors') for i in range(df_mapping.shape[0])])
  # Filtering out only clean records
  #.where(f.col("_rescued_data").isNull())
  .withColumnRenamed("input_event", "input_event_kf")
)

# COMMAND ----------

df_frames.count()

# COMMAND ----------

# Left joining the mtfrp table to the motion_sequence_batting dataframe
joined_df = (
  df_frames_processed.join(
    df_motion,
    (df_frames_processed.input_event_kf == df_motion.input_event)
    & (df_frames_processed.row_number == df_motion.row_number),
    'inner'
  )
  .drop("input_event_kf")
)

# COMMAND ----------

joined_df.count()

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

enriched_df.count()

# COMMAND ----------

# Create a list of fields to be zipped per joint
tracked_joints = df_mapping['motion_sequence'].values.tolist()
untracked_joints = ["JointSpine", "JointHead", "JointLeftUpperArm", "JointRightUpperArm", "JointLeftThigh", "JointRightThigh", "Knob", "Top", "Center"]
motion_measures = ["r11", "r12", "r13", "tx", "r21", "r22", "r23", "ty", "r31", "r32", "r33", "tz", "m41", "m42", "m43", "m44"]
frame_measures = ['TrackingStates', 'ConfidenceValues', 'BoneFittingErrors']

unique_joints = [joint.lower() for joint in tracked_joints if "_dup" not in joint.lower()] + untracked_joints
cols_to_zip = []
for joint in unique_joints:
  if joint not in untracked_joints:
    joint_zip = []
    for measure in motion_measures + frame_measures:
      joint_zip.append(f"{joint}_{measure}")
    cols_to_zip.append(joint_zip)
  elif joint in untracked_joints:
    joint_zip = []
    for measure in motion_measures:
      joint_zip.append(f"{joint}_{measure}")
    cols_to_zip.append(joint_zip)

# COMMAND ----------

# Duplicate columns that are not needed in the resultant master dataframe
cols_to_drop = [elem for sublst in cols_to_zip for elem in sublst] + ['TrackingStates', 'ConfidenceValues', 'BoneFittingErrors'] + ['head_dup_TrackingStates', 'lefthand_dup_TrackingStates', 'righthand_dup_TrackingStates', 'leftfoot_dup_TrackingStates', 'rightfoot_dup_TrackingStates', 'head_dup_ConfidenceValues', 'lefthand_dup_ConfidenceValues', 'righthand_dup_ConfidenceValues', 'leftfoot_dup_ConfidenceValues', 'rightfoot_dup_ConfidenceValues', 'head_dup_BoneFittingErrors', 'lefthand_dup_BoneFittingErrors', 'righthand_dup_BoneFittingErrors', 'leftfoot_dup_BoneFittingErrors', 'rightfoot_dup_BoneFittingErrors']

# COMMAND ----------

# The code below zips together all transformation matrix elements with their corresponding confidence values and fitting errors, to assure usage quality of each record during analysis
df_zipped = (
  enriched_df
  .select("*", *[f.create_map(*list(chain(*[[f.lit(c.split('_')[1]), f.col(c)] for c in joint_zip]))).alias(f"{joint_zip[0].split('_')[0]}_zip") for joint_zip in cols_to_zip])
  # Drop unwanted columns
  .drop(*cols_to_drop)
  .drop('row_index')
  .drop('row_number')
  .drop('_rescued_data')
)

# COMMAND ----------

df_zipped.display()

# COMMAND ----------

df_zipped.count()

# COMMAND ----------

(
  df_zipped
  .write
  .mode("append")
  .format("delta")
  .option("mergeSchema", "true")
  .saveAsTable(table_silver)
)
