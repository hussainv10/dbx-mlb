# Databricks notebook source
# DBTITLE 1,Variables
dbutils.widgets.dropdown("reset_all_data", "0", ["1", "0"], "Reset all data")
dbutils.widgets.text(name="catalog", defaultValue="mlb_gumbo", label="Catalog")

# COMMAND ----------

# Imports
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Global Variables
# Reset All Data
RESET_ALL_DATA = bool(int(dbutils.widgets.get("reset_all_data")))
print(f"Reset All Data: {RESET_ALL_DATA}")

# Global variables
CATALOG = dbutils.widgets.get("catalog")
DATABASE_L = 'landing'
DATABASE_B = 'bronze'
DATABASE_S = 'silver'
DATABASE_G = 'gold'

# Data Location
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{DATABASE_L}/mlb_gumbo_checkpoints"

# COMMAND ----------

bronze_tables = list(spark.sql(f"""SHOW TABLES IN {CATALOG}.bronze""").select('tableName').toPandas()['tableName'].values)
silver_tables = list(spark.sql(f"""SHOW TABLES IN {CATALOG}.silver""").select('tableName').toPandas()['tableName'].values)

# COMMAND ----------

# Set the flag to validate all data
validate_all_data = True

# Check if validation is required
if validate_all_data:
  # Print details for Bronze Tables
  print("Bronze Tables:\n")
  for table in bronze_tables:
    # Print the table name
    print(f'Table: {table}')
    # Read the table into a DataFrame
    df = spark.read.table(f'{CATALOG}.bronze.{table}')
    # Print the number of rows in the table
    print(f'Number of rows: {df.count()}')
    # Print the number of unique events in the table
    print('===========================================================')
    print()
    
  # Print details for Silver Tables
  print("\nSilver Tables:\n")
  for table in silver_tables:
    # Print the table name
    print(f'Table: {table}')
    # Read the table into a DataFrame
    df = spark.read.table(f'{CATALOG}.silver.{table}')
    # Print the number of rows in the table
    print(f'Number of rows: {df.count()}')
    print('===========================================================')
    print()

# COMMAND ----------

# Check the flag to reset all data
if RESET_ALL_DATA:
  spark.sql(f"""DROP DATABASE IF EXISTS {CATALOG}.{DATABASE_S} CASCADE""")
  print("Deleted SILVER database!")
  spark.sql(f"""DROP DATABASE IF EXISTS {CATALOG}.{DATABASE_B} CASCADE""")
  print("Deleted BRONZE database!")

  # Delete Data Checkpoints
  dbutils.fs.rm(CHECKPOINT_BASE, True)
  print("Checkpoints cleared!")

# COMMAND ----------

# MAGIC %md
# MAGIC # Testing Delta Sharing as the Recipient
# MAGIC - Gumbo data shared from GCP workspace
# MAGIC - Two versions of the table - with and without CDF

# COMMAND ----------

table_no_cdc = "mlb_gumbo_gcp.silver.game_data_no_cdc"

df1 = spark.read.table(table_no_cdc)
display(df1)

# COMMAND ----------

# Define the table name
table_cdc = "mlb_gumbo_gcp.silver.game_data"

# Configure the streaming read
change_stream = (
  spark.readStream
  .format("deltaSharing")
  .option("readChangeFeed", "true")
  .table(table_cdc)
)

display(change_stream)

# COMMAND ----------

386 + 34

# COMMAND ----------

df_cdc_batch = (spark.read
    .format("deltaSharing")
    .option("readChangeFeed", "true")  # Enable reading the change data feed
    .option("startingVersion", 4)  # Optional starting timestamp
    .option("endingVersion", 5)  # Optional ending timestamp
    .table("mlb_gumbo_gcp.silver.game_data")
)

display(df_cdc_batch)

# COMMAND ----------

df_cdc_batch = (spark.read
    .format("deltaSharing")
    .option("readChangeFeed", "true")  # Enable reading the change data feed
    .option("startingVersion", 5)  # Optional starting timestamp
    .option("endingVersion", 10)  # Optional ending timestamp
    .table("mlb_gumbo_gcp.silver.game_data")
)

display(df_cdc_batch)

# COMMAND ----------


