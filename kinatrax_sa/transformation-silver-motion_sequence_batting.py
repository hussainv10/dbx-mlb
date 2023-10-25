# Databricks notebook source
# MAGIC %run ./00_setup

# COMMAND ----------

# Imports have already been set in the 00_setup notebook

# Pipeline and task variables
TABLE = 'motion_sequence_batting_p80'
EVENT_TYPE = 'batting'
print(f"Table: {TABLE}")
print(f"Event Type: {EVENT_TYPE}")

# Path variables
table_silver = f'{CATALOG}.{DATABASE_S}.{EVENT_TYPE}_{TABLE}'
checkpoint_location_silver = f'{CHECKPOINT_BASE}/{DATABASE_S}/{EVENT_TYPE}_{TABLE}_checkpoint'
table_bronze_msb = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_motion_sequence_batting'
table_bronze_mtfrp = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_motion_tracker_frame_result_parameters'
table_bronze_bps = f'{CATALOG}.{DATABASE_B}.{EVENT_TYPE}_batter_parameter_set'

# Configuration variables
print(f"Bronze Table 1: {table_bronze_msb}")
print(f"Bronze Table 2: {table_bronze_mtfrp}")
print(f"Bronze Table 3: {table_bronze_bps}")
print(f"Silver Table: {table_silver}")
print(f"Silver Checkpoint Location: {checkpoint_location_silver}")

# COMMAND ----------

# Streaming in the motion_sequence_batting bronze table
df_motion = (
  spark.readStream
  .format('delta')
  .table(f'{table_bronze_msb}')
  # Filtering out only clean records
  .where(f.col("_rescued_data").isNull())
  .drop(f.col("_rescued_data"))
)

# COMMAND ----------

# Streaming in the motion_tracker_frame_result_parameters bronze table
df_frames = (
  spark.readStream
  .format('delta')
  .table(f'{table_bronze_mtfrp}')
  # Filtering out only clean records
  .where(f.col("_rescued_data").isNull())
  .drop(f.col("_rescued_data"))
)

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
)

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

# COMMAND ----------

# Transforming the mtfrp dataframe
df_frames_processed = (
  df_frames
  # Add filter for where _rescued_data column is Null
  .withColumnRenamed(df_frames.columns[1], 'TrackingStates')
  .withColumnRenamed(df_frames.columns[2], 'ConfidenceValues')
  .withColumnRenamed(df_frames.columns[3], 'BoneFittingErrors')
  .withColumn("TrackingStates", f.regexp_replace("TrackingStates", "[{}]", ""))
  .withColumn("TrackingStates", f.split(f.col('TrackingStates'), ";"))
  .withColumn("ConfidenceValues", f.regexp_replace("ConfidenceValues", "[{}]", ""))
  .withColumn("ConfidenceValues", f.split(f.col('ConfidenceValues'), ";"))
  .withColumn("BoneFittingErrors", f.regexp_replace("BoneFittingErrors", "[{}]", ""))
  .withColumn("BoneFittingErrors", f.split(f.col('BoneFittingErrors'), ";"))
  .select("*", *[f.col("TrackingStates").getItem(i).alias(f'{df_mapping["motion_sequence"][i]}_TrackingStates') for i in range(df_mapping.shape[0])])
  .select("*", *[f.col("ConfidenceValues").getItem(i).alias(f'{df_mapping["motion_sequence"][i]}_ConfidenceValues') for i in range(df_mapping.shape[0])])
  .select("*", *[f.col("BoneFittingErrors").getItem(i).alias(f'{df_mapping["motion_sequence"][i]}_BoneFittingErrors') for i in range(df_mapping.shape[0])])
  # Filtering out only clean records
  .where(f.col("_rescued_data").isNull())
  .withColumnRenamed("input_event", "input_event_kf")
)

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

enriched_df.display()

# COMMAND ----------

tracked_joints = df_mapping['motion_sequence'].values.tolist()
untracked_joints = ["JointSpine", "JointHead", "JointLeftUpperArm", "JointRightUpperArm", "JointLeftThigh", "JointRightThigh"]
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

print(cols_to_zip)

# COMMAND ----------

cols_to_drop = [elem for sublst in cols_to_zip for elem in sublst] + ['TrackingStates', 'ConfidenceValues', 'BoneFittingErrors'] + ['head_dup_TrackingStates', 'lefthand_dup_TrackingStates', 'righthand_dup_TrackingStates', 'leftfoot_dup_TrackingStates', 'rightfoot_dup_TrackingStates', 'head_dup_ConfidenceValues', 'lefthand_dup_ConfidenceValues', 'righthand_dup_ConfidenceValues', 'leftfoot_dup_ConfidenceValues', 'rightfoot_dup_ConfidenceValues', 'head_dup_BoneFittingErrors', 'lefthand_dup_BoneFittingErrors', 'righthand_dup_BoneFittingErrors', 'leftfoot_dup_BoneFittingErrors', 'rightfoot_dup_BoneFittingErrors']
print(cols_to_drop)

# COMMAND ----------

# f.create_map(*list(chain(*[[f.lit(c), f.col(c)] for c in joint_zip])))
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

# MAGIC %md
# MAGIC TODO: 
# MAGIC 1. Write out ('append') the results of the joined table to the target DataFrame
# MAGIC 8. Test the pipeline run end-to-end against new data.
# MAGIC 9. Clean up the lookup table and other config data that can be abstracted away from the pure processing of this pipeline. Also use the final tested output DataFrame as the schema used for the CTAS (since liquid clustering can only be defined during CTAS). Can new columns be merged into a LC table defined with fewer fields on CTAS?
# MAGIC
# MAGIC
# MAGIC Replicate the same process for the motion_sequence_bat and motion_sequence_ball and their respective frame_result_parameter tables:
# MAGIC
# MAGIC a. For bat - both files used in the join have the same number of rows
# MAGIC
# MAGIC b. For ball - both files have different number of rows - Filter out Tracked = True on frame data before joining to ball motion seq data
# MAGIC
# MAGIC c. Maybe these don't have to be replicated processes. They can just stream from the master batting table after selecting out the relevant bat and ball columns

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

# COMMAND ----------


