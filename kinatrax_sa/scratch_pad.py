# Databricks notebook source
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


