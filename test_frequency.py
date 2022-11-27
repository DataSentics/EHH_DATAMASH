# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

labs_data_orig=spark.read.table('labs_all')

# COMMAND ----------

labs_data_orig = spark.read.table('labeled_dataset')

# COMMAND ----------

labs_data_orig.select(F.col("Patient")).distinct().count()

# COMMAND ----------

display(labs_data_orig.filter(F.col("Patient")==938).filter(F.col("NCLP")==17339))

# COMMAND ----------

NCLP_patients = (
    labs_data_orig
    .filter(F.col('NCLP')==17339)
    .filter(F.col('Unit').isin(['ml/s/spt','ml/s/1,73 m2']))
)

# COMMAND ----------

display(NCLP_patients.filter(F.col("Patient")==938).filter(F.col("NCLP")==17339))

# COMMAND ----------

bmi_df = spark.read.table('bmi_dataset')

# COMMAND ----------

w = Window.orderBy("date").partitionBy('Patient')
bmi_df = (bmi_df.withColumn('date_lagged', F.lag('date').over(w))
              .withColumn('date_lagged', F.when(F.col('date_lagged').isNull(), F.col('date')).otherwise(F.col('date_lagged')))
             # .drop('Weight', 'Height')
         )

# COMMAND ----------

bsa_calc = bmi_df.withColumn('BSA', F.sqrt((F.col('Height')*F.col('Weight'))/3600))
bsa_calc = bmi_df.withColumn('BSA', 0.007184*F.pow(F.col('Height'), F.lit(0.725))*F.pow(F.col('Weight'),F.lit(0.425)))

# COMMAND ----------

display(bsa_calc)

# COMMAND ----------

bsa_calc = bsa_calc.groupBy('Patient').agg(F.max('BSA').alias('BSA')).dropna()

# COMMAND ----------

df = NCLP_patients.join(bsa_calc, on='Patient', how='inner')

# COMMAND ----------

bmi_df.select(F.col("Patient")).distinct().count()

# COMMAND ----------

display(bsa_calc.filter(F.col("Patient")==938))

# COMMAND ----------

bmi_df

# COMMAND ----------

bsa_calc = bmi_df.withColumn('BSA', 0.007184*F.pow(F.col('Height'), F.lit(0.725))*F.pow(F.col('Weight'),F.lit(0.425)))
bsa_calc = bsa_calc.groupBy('Patient').agg(F.max('BSA').alias('BSA')).dropna()
df = NCLP_patients.join(bsa_calc, on='Patient', how='inner')

# COMMAND ----------

display(labs_data_orig)

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
