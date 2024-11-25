# Databricks notebook source
# Imports
import pyspark.sql.functions as F

# COMMAND ----------

# Global variables
CURRENT_USER = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
CATALOG = 'mlb_gumbo'
DATABASE_L = 'landing'
DATABASE_B = 'bronze'
DATABASE_S = 'silver'
DATABASE_G = 'gold'

# Data Location
SCHEMA_BASE = f'dbfs:/user/{CURRENT_USER}/{CATALOG}'
CHECKPOINT_BASE = f'dbfs:/user/{CURRENT_USER}/{CATALOG}'
