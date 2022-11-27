# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

df_labeled = spark.read.table(
  'hive_metastore.default.labeled_dataset'
)

# COMMAND ----------

display(df_labeled.where(F.col("Patient")==938).select("Patient", "17339"))

# COMMAND ----------

display(df_nclp_patients.where(F.col("Patient")==938))

# COMMAND ----------

df_labeled.select("Patient").distinct().count()

# COMMAND ----------

df_nclp_patients = spark.read.table(
  'hive_metastore.default.nclp_patients'
)

# COMMAND ----------

nclp_patients = df_nclp_patients.select("Patient").distinct().toPandas().Patient.values

# COMMAND ----------

tests = ["17339", "17341", "14845", "18066", "1450", "1451", "14691", "8553", "8574", "8572", "3086", "3084"]

# COMMAND ----------

df_rel = df_labeled.select(["Patient"] + tests)

# COMMAND ----------

display(df_rel)

# COMMAND ----------

df_rel_grouped = (
    df_rel
    .groupBy("Patient")
    .agg(
        *[(F.max(F.col(x).isNotNull().cast("int"))).alias(x) for x in tests]
    )
)

# COMMAND ----------

display(df_rel_grouped)

# COMMAND ----------

pdf = df_rel_grouped.toPandas()

# COMMAND ----------

lab_patients = pdf[pdf["17339"]==True].Patient.values

# COMMAND ----------

set(lab_patients.tolist()) -  set(nclp_patients.tolist())

# COMMAND ----------

pdf["17339"].sum()

# COMMAND ----------

pdf["17339"].sum()
