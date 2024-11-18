# Databricks notebook source
print("Hello Serverless")

# COMMAND ----------

x = 2
y = 3
z = x + y
print(z)

# COMMAND ----------

z

# COMMAND ----------

# MAGIC %pip install tensorflow

# COMMAND ----------

# Clearing state and running all again preserves notebook-scoped installed libraries
# Detaching the notebook from the Serverless endpoint clears all state but not installed libraries. This is great because detaching and re-attaching from Classic compute clusters clears state and libraries

# COMMAND ----------

# Spark UI is different for queries that run on Serverless vs Classic.Serverless one is cleaner, query based.

# COMMAND ----------

df = spark.read.table('kinatrax.bronze.batting_batter_parameter_set')
display(df)

# COMMAND ----------

# AWS Docs
- https://docs.databricks.com/en/compute/serverless.html
- https://docs.databricks.com/en/workflows/jobs/run-serverless-jobs.html
- https://docs.databricks.com/en/release-notes/serverless.html

# COMMAND ----------

# MAGIC %md
# MAGIC Current limitations that we need to be aware of (ref Release Notes):
# MAGIC - Same limitations as “Shared Access” mode clusters
# MAGIC - No cluster configs and environment variables (a lot of common spark configs are not available, input_file_name() also not available, switch to file metadata instead)
# MAGIC - No RDDs, Spark Context, sqlContext
# MAGIC Continuous streaming (batch streaming with Autoloader and Trigger.AvailableNow should work, to be tested)
# MAGIC - No DBFS (that’s fine, access UC volumes instead)
# MAGIC - No ML Runtime or GPUs (ML libraries can be pip installed though)
# MAGIC - No R
# MAGIC - No custom data sources (e.g. JDBC, MongoDB)
# MAGIC - No caching of DataFrames (df.persist, df.cache etc.)
# MAGIC - No Global Temp Views

# COMMAND ----------


