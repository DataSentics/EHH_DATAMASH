# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

labs_data_df = spark.read.table('labs_all')
display(labs_data_df)

# COMMAND ----------

"30.12.1899 11:38:00"

# COMMAND ----------

transformed_labs_data_df = (
    labs_data_df
    .withColumn("Value", F.coalesce(F.col("ValueNumber"), F.col("ValueText")))
    .withColumn("Time", F.to_timestamp("EntryTime"))
    .select(["Patient", "EntryDate", "EntryTime", "Time", "NCLP", "Value"])
)

# COMMAND ----------

display(transformed_labs_data_df)

# COMMAND ----------

display(labs_data_df.count())

# COMMAND ----------

display(labs_data_df.select("ID").distinct().count())

# COMMAND ----------

display(labs_data_df.select("Report").distinct().count())

# COMMAND ----------

display(labs_data_df.select("Patient"))

# COMMAND ----------

display(labs_data_df.select("Code", "NCLP", "Analyte").group)

# COMMAND ----------

labs_data_df.filter(col("NCLP").isNotNull()).count()

# COMMAND ----------

69343 / 979175

# COMMAND ----------

labs_data_df.filter(col("Code").isNotNull()).count()

# COMMAND ----------

display(labs_data_df.select("NCLP").distinct().count())

# COMMAND ----------

diagnosis_df = spark.read.table('dg_from_report')
display(diagnosis_df)

# COMMAND ----------

# quick test
display(labs_data_df.filter(col("Patient")==1191501))
display(diagnosis_df.filter(col("Patient")==1191501))

# COMMAND ----------


