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

joints_txt = "Timestamp Hips_R11 Hips_R12 Hips_R13 Hips_tx Hips_R21 Hips_R22 Hips_R23 Hips_ty Hips_R31 Hips_R32 Hips_R33 Hips_tz Hips_M41 Hips_M42 Hips_M43 Hips_M44 JointSpine_R11 JointSpine_R12 JointSpine_R13 JointSpine_tx JointSpine_R21 JointSpine_R22 JointSpine_R23 JointSpine_ty JointSpine_R31 JointSpine_R32 JointSpine_R33 JointSpine_tz JointSpine_M41 JointSpine_M42 JointSpine_M43 JointSpine_M44 Spine_R11 Spine_R12 Spine_R13 Spine_tx Spine_R21 Spine_R22 Spine_R23 Spine_ty Spine_R31 Spine_R32 Spine_R33 Spine_tz Spine_M41 Spine_M42 Spine_M43 Spine_M44 Neck_R11 Neck_R12 Neck_R13 Neck_tx Neck_R21 Neck_R22 Neck_R23 Neck_ty Neck_R31 Neck_R32 Neck_R33 Neck_tz Neck_M41 Neck_M42 Neck_M43 Neck_M44 JointHead_R11 JointHead_R12 JointHead_R13 JointHead_tx JointHead_R21 JointHead_R22 JointHead_R23 JointHead_ty JointHead_R31 JointHead_R32 JointHead_R33 JointHead_tz JointHead_M41 JointHead_M42 JointHead_M43 JointHead_M44 Head_R11 Head_R12 Head_R13 Head_tx Head_R21 Head_R22 Head_R23 Head_ty Head_R31 Head_R32 Head_R33 Head_tz Head_M41 Head_M42 Head_M43 Head_M44 JointLeftUpperArm_R11 JointLeftUpperArm_R12 JointLeftUpperArm_R13 JointLeftUpperArm_tx JointLeftUpperArm_R21 JointLeftUpperArm_R22 JointLeftUpperArm_R23 JointLeftUpperArm_ty JointLeftUpperArm_R31 JointLeftUpperArm_R32 JointLeftUpperArm_R33 JointLeftUpperArm_tz JointLeftUpperArm_M41 JointLeftUpperArm_M42 JointLeftUpperArm_M43 JointLeftUpperArm_M44 LeftUpperArm_R11 LeftUpperArm_R12 LeftUpperArm_R13 LeftUpperArm_tx LeftUpperArm_R21 LeftUpperArm_R22 LeftUpperArm_R23 LeftUpperArm_ty LeftUpperArm_R31 LeftUpperArm_R32 LeftUpperArm_R33 LeftUpperArm_tz LeftUpperArm_M41 LeftUpperArm_M42 LeftUpperArm_M43 LeftUpperArm_M44 LeftForearm_R11 LeftForearm_R12 LeftForearm_R13 LeftForearm_tx LeftForearm_R21 LeftForearm_R22 LeftForearm_R23 LeftForearm_ty LeftForearm_R31 LeftForearm_R32 LeftForearm_R33 LeftForearm_tz LeftForearm_M41 LeftForearm_M42 LeftForearm_M43 LeftForearm_M44 LeftWrist_R11 LeftWrist_R12 LeftWrist_R13 LeftWrist_tx LeftWrist_R21 LeftWrist_R22 LeftWrist_R23 LeftWrist_ty LeftWrist_R31 LeftWrist_R32 LeftWrist_R33 LeftWrist_tz LeftWrist_M41 LeftWrist_M42 LeftWrist_M43 LeftWrist_M44 LeftHand_R11 LeftHand_R12 LeftHand_R13 LeftHand_tx LeftHand_R21 LeftHand_R22 LeftHand_R23 LeftHand_ty LeftHand_R31 LeftHand_R32 LeftHand_R33 LeftHand_tz LeftHand_M41 LeftHand_M42 LeftHand_M43 LeftHand_M44 JointRightUpperArm_R11 JointRightUpperArm_R12 JointRightUpperArm_R13 JointRightUpperArm_tx JointRightUpperArm_R21 JointRightUpperArm_R22 JointRightUpperArm_R23 JointRightUpperArm_ty JointRightUpperArm_R31 JointRightUpperArm_R32 JointRightUpperArm_R33 JointRightUpperArm_tz JointRightUpperArm_M41 JointRightUpperArm_M42 JointRightUpperArm_M43 JointRightUpperArm_M44 RightUpperArm_R11 RightUpperArm_R12 RightUpperArm_R13 RightUpperArm_tx RightUpperArm_R21 RightUpperArm_R22 RightUpperArm_R23 RightUpperArm_ty RightUpperArm_R31 RightUpperArm_R32 RightUpperArm_R33 RightUpperArm_tz RightUpperArm_M41 RightUpperArm_M42 RightUpperArm_M43 RightUpperArm_M44 RightForearm_R11 RightForearm_R12 RightForearm_R13 RightForearm_tx RightForearm_R21 RightForearm_R22 RightForearm_R23 RightForearm_ty RightForearm_R31 RightForearm_R32 RightForearm_R33 RightForearm_tz RightForearm_M41 RightForearm_M42 RightForearm_M43 RightForearm_M44 RightWrist_R11 RightWrist_R12 RightWrist_R13 RightWrist_tx RightWrist_R21 RightWrist_R22 RightWrist_R23 RightWrist_ty RightWrist_R31 RightWrist_R32 RightWrist_R33 RightWrist_tz RightWrist_M41 RightWrist_M42 RightWrist_M43 RightWrist_M44 RightHand_R11 RightHand_R12 RightHand_R13 RightHand_tx RightHand_R21 RightHand_R22 RightHand_R23 RightHand_ty RightHand_R31 RightHand_R32 RightHand_R33 RightHand_tz RightHand_M41 RightHand_M42 RightHand_M43 RightHand_M44 JointLeftThigh_R11 JointLeftThigh_R12 JointLeftThigh_R13 JointLeftThigh_tx JointLeftThigh_R21 JointLeftThigh_R22 JointLeftThigh_R23 JointLeftThigh_ty JointLeftThigh_R31 JointLeftThigh_R32 JointLeftThigh_R33 JointLeftThigh_tz JointLeftThigh_M41 JointLeftThigh_M42 JointLeftThigh_M43 JointLeftThigh_M44 LeftThigh_R11 LeftThigh_R12 LeftThigh_R13 LeftThigh_tx LeftThigh_R21 LeftThigh_R22 LeftThigh_R23 LeftThigh_ty LeftThigh_R31 LeftThigh_R32 LeftThigh_R33 LeftThigh_tz LeftThigh_M41 LeftThigh_M42 LeftThigh_M43 LeftThigh_M44 LeftShin_R11 LeftShin_R12 LeftShin_R13 LeftShin_tx LeftShin_R21 LeftShin_R22 LeftShin_R23 LeftShin_ty LeftShin_R31 LeftShin_R32 LeftShin_R33 LeftShin_tz LeftShin_M41 LeftShin_M42 LeftShin_M43 LeftShin_M44 LeftAnkle_R11 LeftAnkle_R12 LeftAnkle_R13 LeftAnkle_tx LeftAnkle_R21 LeftAnkle_R22 LeftAnkle_R23 LeftAnkle_ty LeftAnkle_R31 LeftAnkle_R32 LeftAnkle_R33 LeftAnkle_tz LeftAnkle_M41 LeftAnkle_M42 LeftAnkle_M43 LeftAnkle_M44 LeftFoot_R11 LeftFoot_R12 LeftFoot_R13 LeftFoot_tx LeftFoot_R21 LeftFoot_R22 LeftFoot_R23 LeftFoot_ty LeftFoot_R31 LeftFoot_R32 LeftFoot_R33 LeftFoot_tz LeftFoot_M41 LeftFoot_M42 LeftFoot_M43 LeftFoot_M44 JointRightThigh_R11 JointRightThigh_R12 JointRightThigh_R13 JointRightThigh_tx JointRightThigh_R21 JointRightThigh_R22 JointRightThigh_R23 JointRightThigh_ty JointRightThigh_R31 JointRightThigh_R32 JointRightThigh_R33 JointRightThigh_tz JointRightThigh_M41 JointRightThigh_M42 JointRightThigh_M43 JointRightThigh_M44 RightThigh_R11 RightThigh_R12 RightThigh_R13 RightThigh_tx RightThigh_R21 RightThigh_R22 RightThigh_R23 RightThigh_ty RightThigh_R31 RightThigh_R32 RightThigh_R33 RightThigh_tz RightThigh_M41 RightThigh_M42 RightThigh_M43 RightThigh_M44 RightShin_R11 RightShin_R12 RightShin_R13 RightShin_tx RightShin_R21 RightShin_R22 RightShin_R23 RightShin_ty RightShin_R31 RightShin_R32 RightShin_R33 RightShin_tz RightShin_M41 RightShin_M42 RightShin_M43 RightShin_M44 RightAnkle_R11 RightAnkle_R12 RightAnkle_R13 RightAnkle_tx RightAnkle_R21 RightAnkle_R22 RightAnkle_R23 RightAnkle_ty RightAnkle_R31 RightAnkle_R32 RightAnkle_R33 RightAnkle_tz RightAnkle_M41 RightAnkle_M42 RightAnkle_M43 RightAnkle_M44 RightFoot_R11 RightFoot_R12 RightFoot_R13 RightFoot_tx RightFoot_R21 RightFoot_R22 RightFoot_R23 RightFoot_ty RightFoot_R31 RightFoot_R32 RightFoot_R33 RightFoot_tz RightFoot_M41 RightFoot_M42 RightFoot_M43 RightFoot_M44 Knob_R11 Knob_R12 Knob_R13 Knob_tx Knob_R21 Knob_R22 Knob_R23 Knob_ty Knob_R31 Knob_R32 Knob_R33 Knob_tz Knob_M41 Knob_M42 Knob_M43 Knob_M44 Top_R11 Top_R12 Top_R13 Top_tx Top_R21 Top_R22 Top_R23 Top_ty Top_R31 Top_R32 Top_R33 Top_tz Top_M41 Top_M42 Top_M43 Top_M44 Center_R11 Center_R12 Center_R13 Center_tx Center_R21 Center_R22 Center_R23 Center_ty Center_R31 Center_R32 Center_R33 Center_tz Center_M41 Center_M42 Center_M43 Center_M44"

# Extract each joint text from the text string
joint_texts = joints_txt.split(" ")
joints = [joint_text.split("_")[0] for joint_text in joint_texts]

# Get unique joints
unique_joints = list(set(joints))

# Print unique joints
print(unique_joints)

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

print(f"Motion: {df_motion.count()}")
print(f"Frames: {df_frames.count()}")

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

df_mapping['motion_sequence'].values.tolist()

# COMMAND ----------

all_joints_mtfrp = df_mapping['motion_sequence'].values.tolist()
unique_joints_mtfrp = [joint.lower() for joint in all_joints_mtfrp if "_dup" not in joint.lower()]
cols_to_zip = []
for joint in unique_joints_mtfrp:
  joint_zip = []
  for measure in ['TrackingStates', 'ConfidenceValues', 'BoneFittingErrors']:
    joint_zip.append(f"{joint}_{measure}")
  cols_to_zip.append(joint_zip)
print(cols_to_zip)

# COMMAND ----------

import pyspark.sql.functions as f

# f.create_map(*list(chain(*[[f.lit(c), f.col(c)] for c in joint_zip])))
df_frames4 = (
  df_frames2.select("*", *[f.create_map(*list(chain(*[[f.lit(c.split('_')[1]), f.col(c)] for c in joint_zip]))).alias(f"{joint_zip[0].split('_')[0]}_zip") for joint_zip in cols_to_zip])
)
df_frames4.display()

# COMMAND ----------

cols_to_zip_motion = [col.lower() for col in df_motion.columns if "righthand" in col.lower() and "_dup" not in col.lower()]
cols_to_zip_motion

# COMMAND ----------

all_joints_motion = df_mapping['motion_sequence'].values.tolist() + ["JointSpine", "JointHead", "JointLeftUpperArm", "JointRightUpperArm", "JointLeftThigh", "JointRightThigh"]

unique_joints_motion = [joint.lower() for joint in all_joints_motion if "_dup" not in joint.lower()]
cols_to_zip_motion = []
for joint in unique_joints_motion:
  joint_zip = []
  for measure in ["r11", "r12", "r13", "tx", "r21", "r22", "r23", "ty", "r31", "r32", "r33", "tz", "m41", "m42", "m43", "m44"]:
    joint_zip.append(f"{joint}_{measure}")
  cols_to_zip_motion.append(joint_zip)
print(len(cols_to_zip_motion))
print(cols_to_zip_motion)

# COMMAND ----------

df_motion2 = (
  df_motion
  .withColumn("righthand_map", f.create_map(*list(chain(*[[f.lit(c.split('_')[-1]), f.col(c)] for c in cols_to_zip_motion]))))
  .drop("input_file")
)
df_motion2.display()

# COMMAND ----------

df_motion2 = (
  df_motion.select("*", *[f.create_map(*list(chain(*[[f.lit(c.split('_')[1]), f.col(c)] for c in joint_zip]))).alias(f"{joint_zip[0].split('_')[0]}_zip") for joint_zip in cols_to_zip_motion])
)
df_motion2.display()

# COMMAND ----------

df_frames3.count()

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
  .option("checkpointLocation", checkpoint_location_silver+"_tempmotion")
  .outputMode("append")
  .trigger(availableNow=True)
  .table("kinatrax.silver.tempmotion")
)

# COMMAND ----------

static_frames = (
  sdf_frames
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_location_silver+"_tempframes")
  .outputMode("append")
  .trigger(availableNow=True)
  .table("kinatrax.silver.tempframes")
)

# COMMAND ----------

static_df_motion = spark.read.table("kinatrax.silver.tempmotion").withColumn("row_number", f.row_number().over(Window.partitionBy('input_event').orderBy("Timestamp")))
static_df_motion.count()

# COMMAND ----------

static_df_motion.display()

# COMMAND ----------

static_df_frames = spark.read.table("kinatrax.silver.tempframes")
static_df_frames.count()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType

# Define the schema for the dataframe
schema = StructType([StructField("row_index", LongType(), True)] + static_df_frames.schema.fields)

#TODO: Verify that the correct ordering per file is preserved here in this case. If this can't be guaranteed, the following join will yield an incorrect ordering. UNIT TEST THIS! DONE

# Add the row index to the dataframe
static_df_frames2 = static_df_frames.rdd.zipWithIndex().map(lambda row: (row[1],) + row[0]).toDF(schema).withColumn('row_number', f.row_number().over(Window.partitionBy('input_event').orderBy("row_index")))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED kinatrax.silver.tempframes

# COMMAND ----------

# MAGIC %fs ls s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b128f36d/tables/f3225022-7ce4-457d-a2b1-8dedd8e2febf

# COMMAND ----------

import pandas as pd
df = pd.read_csv('/Volumes/kinatrax/landing/all/game1/batting/2023_04_26_19_10_28_Washington_Nationals_17_Alex_Call_Home/motion_tracker_frame_result_parameters.csv')
display(df.head())

# COMMAND ----------

static_df_frames2.display()

# COMMAND ----------

# Joining the two temporary dataframes
static_df_joined = (
  static_df_motion.join(
    static_df_frames2,
    ['input_event', 'row_number'],
    'full'
  )
)
static_df_joined.display()

# COMMAND ----------

#dbutils.fs.head("dbfs:/user/hussain.vahanvaty@databricks.com/kinatrax/silver/batting_motion_sequence_batting_p80_checkpoint_tempstatic/offsets/0")

# COMMAND ----------

#dbutils.fs.rm("dbfs:/user/hussain.vahanvaty@databricks.com/kinatrax/silver/batting_motion_sequence_batting_p80_checkpoint_tempstatic/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: 
# MAGIC 1. Write out ('append') the results of the joined table to the target DataFrame
# MAGIC 2. Clear out ('drop') the temp tables to clear them for the next micro batch
# MAGIC 3. Unit test the zipWithIndex logic to ensure that correct ordering per frame file is maintained. If this isn't the case then the join will be out of order. Another way to test this would also be to check that the timestamps (in increments of 3.33 correspond to the correct row index i.e. 0->1, 3.33->2 etc.) DONE
# MAGIC 4. Extend the grouping logic per joint to extend for all joints. Currently we are generating the consolidated transformation matrix just for the right hand joint. Do this for all the joints. DONE
# MAGIC 5. Ensure that after the join the TrackingStates, ConfidenceValues and BoneFittingError per joint is added to the transformation matrix of each joint.
# MAGIC 6. Also add a confidence threshold filter to create a composite confidence score per row and then filter out rows below a certain composite confidence score (eg. p80)
# MAGIC 7. Join this with game metadata from bronze.batter_parameter_set
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

# Create a test PySpark DataFrame
from pyspark.sql import Row
test_data = [Row(row_index=10, input_event='event1', col1='a'), Row(row_index=20, input_event='event1', col1='b'), Row(row_index=30, input_event='event2', col1='c')]
test_df = spark.createDataFrame(test_data)

# Define expected schema for output DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
expected_schema = StructType([StructField('row_index', IntegerType(), True),
                              StructField('input_event', StringType(), True),
                              StructField('col1', StringType(), True),
                              StructField('row_number', IntegerType(), True)])

# Define expected output DataFrame
expected_data = [(10, 'event1', 'a', 1), (20, 'event1', 'b', 2), (30, 'event2', 'c', 1)]
expected_df = spark.createDataFrame(expected_data, expected_schema)

# Define unit test function
def test_static_df_frames2():
  # Apply transformation to test data
  result_df = (
      test_df.rdd
      .zipWithIndex()
      .map(lambda row: row[0] + (row[1],))
      .toDF(expected_schema)
      .withColumn('row_number', f.row_number().over(Window.partitionBy('input_event').orderBy('row_index')))
  )

  # Assert that the test and expected DataFrames are equal
  print(result_df.collect())
  print(expected_df.collect())
  assert result_df.collect() == expected_df.collect()
  print("TEST PASSED!")

# Run the unit test
test_static_df_frames2()

# COMMAND ----------


