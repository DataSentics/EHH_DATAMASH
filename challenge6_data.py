# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

labs_data_df = spark.read.table('labs_all')
display(labs_data_df)

# COMMAND ----------

new_labs_data_df = (
    labs_data_df
    .withColumn("Time", F.to_timestamp("EntryTime", 'dd.MM.yyyy H:mm:ss'))
    .withColumn("TimeString", F.date_format(F.col('Time'), 'hh:mm:ss'))
    .withColumn("DateString", F.date_format(F.col('EntryDate'), 'dd-MM-yyyy'))
    .withColumn("Entry", F.to_timestamp(F.concat(F.col("DateString"), F.lit(' '),F.col("TimeString")), 'dd-MM-yyyy hh:mm:ss'))
)

display(new_labs_data_df)

# COMMAND ----------

time_df = transformed_labs_data_df.withColumn("Time", F.to_timestamp(F.col("EntryTime"), 'dd.MM.yyyy H:mm:ss'))

# COMMAND ----------

time_df = time_df.withColumn("Date", F.to_timestamp(F.col("EntryDate")))

# COMMAND ----------

display(time_df)

# COMMAND ----------

display(time_df.withColumn('Time2', F.col("Date") + F.date_format(F.col('Time'), 'HH:mm:ss')))

# COMMAND ----------

time_df(F.col("Time"))

# COMMAND ----------

display(time_df.withColumn("Entry", F.col("Time") + F.col("Time")))

# COMMAND ----------

(transformed_labs_data_df = (
    labs_data_df
    .withColumn("Value", F.coalesce(F.col("ValueNumber"), F.col("ValueText")))
    #.withColumn("Time", F.to_timestamp("EntryTime"))
    .select(["Patient", "EntryDate", "EntryTime", "Time", "NCLP", "Value"])
)

# COMMAND ----------

transformed_labs_data_df.()

# COMMAND ----------

df_modified=labs_data_df.withColumn("converted_timestamp",F.to_timestamp("EntryTime", 'dd.MM.yyyy HH:mm:ss'))
MM-dd-yyyy HH:mm:ss.SSS

# COMMAND ----------

display(df_modified)

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


