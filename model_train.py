# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

df_labeled = spark.read.table(
  'hive_metastore.default.labeled_dataset'
)

df_nclp_patients = spark.read.table(
  'hive_metastore.default.nclp_patients'
)

# COMMAND ----------

display(df_nclp_patients.select("Value_adj", "Value"))

# COMMAND ----------

# main table for training
df_tested_patients = (
    df_nclp_patients
    .withColumnRenamed("Value", "target")
    .withColumnRenamed("Patient", "TestedPatient")
    .withColumnRenamed("Entry", "GEntry")
    .select("TestedPatient", "GEntry", "target")
    .filter(F.col("TestedPatient").isNotNull())
)

df_joined = (
    # patients with G test
    df_tested_patients
    # join historical lab tests
    .join(df_labeled, (df_tested_patients.TestedPatient ==  df_labeled.Patient) & (df_tested_patients.GEntry >=  df_labeled.Entry), "left")
    .drop("TestedPatient", "date_lagged", "date")
)

df_joined.cache()
display(df_joined)

# COMMAND ----------

# testing
df_single_test = df_joined.filter(F.col("Patient") == 75232).select("GEntry", "target", "Patient", "Entry", "53")

# COMMAND ----------

w  = Window.partitionBy("Patient", "GEntry").orderBy(F.col("Entry").desc())

# COMMAND ----------

df_latest = (
    df_single_test
    .withColumn("f_53", F.first(F.col("53"), ignorenulls=True).over(w))
    .groupBy("Patient", "GEntry")
    .agg(F.first("f_53", ignorenulls=True))
)
    
    

# COMMAND ----------

# Generic solution for all columns
amount_missing_df = df.select([(count(when(isnan(c) | col(c).isNull(), c))/count(lit(1))).alias(c) for c in df.columns])
amount_missing_df.show()

# COMMAND ----------

#df_latest = df_single_test.groupBy(["Patient", "GEntry"]).agg(F.first(F.col("582"), ignorenulls=True))

# COMMAND ----------

display(df_latest.select("Patient", "GEntry", "Target"))

# COMMAND ----------

from catboost import CatBoostRegressor
# Initialize data

train_data = [[1, 4, 5, 6],
              [4, 5, 6, 7],
              [30, 40, 50, 60]]

eval_data = [[2, 4, 6, 8],
             [1, 4, 50, 60]]

train_labels = [10, 20, 30]
# Initialize CatBoostRegressor
model = CatBoostRegressor(iterations=2,
                          learning_rate=1,
                          depth=2)
# Fit model
model.fit(train_data, train_labels)
# Get predictions
preds = model.predict(eval_data)
