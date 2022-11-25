# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

labs_data_df = spark.read.table('labs_all')
display(labs_data_df)

# COMMAND ----------

display(labs_data_df.select("Patient").distinct())

# COMMAND ----------

diagnosis_df = spark.read.table('dg_from_report')
display(diagnosis_df)

# COMMAND ----------

# quick test
display(labs_data_df.filter(col("Patient")==1191501))
display(diagnosis_df.filter(col("Patient")==1191501))

# COMMAND ----------


