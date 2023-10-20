# Databricks notebook source
# MAGIC %md
# MAGIC IMPORTANT OBSERVATIONS SO FAR:
# MAGIC - So far we have uploaded and are using batting data for all batters on **one team in one game**
# MAGIC - Liquid clustering cannot be applied to an existing Delta table. It has to be enabled on a table as a table property during creation.
# MAGIC - Reconciliation of motion_sequence_batting.csv and motion_sequence.csv files shows that the motion_sequence_batting.csv file already contains batter + bat + ball data joined together while the motion_sequence.csv file contains just the player body data
# MAGIC
# MAGIC
# MAGIC QUESTIONS:
# MAGIC - ##### All columns are being read as text columns even with the schema inference option set to True. How can we work around this?
# MAGIC   - Should bronze tables perform schema inference OR should bronze tables just have string columns?
# MAGIC     - Just string columns to preserve speed and flexibility for downstream usage. Autoloader schema inference only detects fields and not field types.
# MAGIC   - If the bronze table is required to use liquid clustering, schema has to be defined once intially during the CTAS statement
# MAGIC
# MAGIC - ##### For the 4 x 4 transformation matrices, what is the origin from which the transformations are computed?
# MAGIC
# MAGIC - ##### When using Autoloader with Trigger.AvailableNow and maxFilesPerBatch, how is this handled? Does this mean that only one batch is processed or are all outstanding files picked up by the trigger chunked up into batches, processed, and then finally terminated once all chunks have been processed?
# MAGIC
# MAGIC - ##### Can new columns be merged into a LC table defined with fewer fields on CTAS?
# MAGIC
# MAGIC
# MAGIC
# MAGIC TASKS:
# MAGIC - ##### Setup
# MAGIC   - Store raw data.
# MAGIC     - This should be the same open S3/ADLS bucket used for solution accelerators on the Databricks website.
# MAGIC     - Maybe store the raw data in git (probably too large) or host on some FTP server where it can be downloaded from.
# MAGIC     - Use UC API/CLI to create a landing schema and 'Volume' inside customer's workspace (from an external location if using open bucket else a managed one to download FTP files to if doing a direct download)
# MAGIC     - Slide/paragraph summarizing the structure of the raw data
# MAGIC     - Automated script to simulate new files landing into the 'landing zone' which the accelerator will feed off of, perhaps as a separate job that the user can run asynchronously.
# MAGIC
# MAGIC
# MAGIC   - Packaging code
# MAGIC     - Consolidate helper functions and global variables into a separate notebook/.py file
# MAGIC     - Add CTAS table definitions with Liquid Clustering properties to a separate config notebook
# MAGIC     - Package workflow/AutoML/dashboard API code into separate scripts
# MAGIC     - Use dbdemos infrastructure installation and setup template to auotmate infra creation
# MAGIC     - Package all this code into a Python package that can be pip installed along with library/Runtime dependencies
# MAGIC     - Final documentation/slide on installation and setup process

# COMMAND ----------

# Read txt file with Spark in UC Volumes
file_path = '/Volumes/hussain_v/kinatrax_demo_landing/batting/motion_sequence.csv'
test_df = spark.read.option("header", "true").option("delimiter", " ").csv(file_path)
test_df.display()

# COMMAND ----------

file_path_txt = '/Volumes/hussain_v/kinatrax_demo_landing/batting/2023_04_26_19_10_28_Washington_Nationals_17_Alex_Call_Home.report.txt'
df_txt = spark.read.text(file_path_txt)
df_txt.display()

# COMMAND ----------

test_df2 = spark.read.option("header", "true").option("delimiter", "\t").csv(file_path_txt)
test_df.display()

# COMMAND ----------

xml_path = '/Volumes/hussain_v/kinatrax_demo_landing/batting/track_joint_center_set_properties.xml'

df_xml = (spark.read
    .format("com.databricks.spark.xml")
    .option("rootTag", "TrackJointCenterSetProperties")
    .option("rowTag", "TrackJointCenterSetProperties")
    .load(xml_path)
)

df_xml.display()

# COMMAND ----------


