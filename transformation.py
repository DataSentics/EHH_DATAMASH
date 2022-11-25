# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosis

# COMMAND ----------

diagnosis_df = spark.read.table('dg_from_report')
display(diagnosis_df)

# COMMAND ----------

diagnosis_main_df = (diagnosis_df
                     .withColumn("Year", F.year("Date"))
                     .selectExpr("Patient", "Year", "mainDgCode as Description")
                    )
diagnosis_other_df = (diagnosis_df
                      .withColumn("Year", F.year("Date"))
                      .selectExpr("Patient", "Year", "OtherDgCode as Description")
                     )
diagnosis_joint_df = diagnosis_main_df.union(diagnosis_other_df)

# COMMAND ----------

diagnosis_df_codes = (diagnosis_joint_df
                      .distinct()
                      .withColumn("Description", F.lower(F.col('Description')))
                      .withColumn('code', F.substring(F.col('Description'), 0,3))
                      .withColumn("is_CKD", F.when(
                          (F.col("code") == "n18") | (F.col("code") == "n19") |
                          F.col("Description").contains('chronické onemocnění ledvin') |
                          F.col("Description").contains("ckd")
                          , 1).otherwise(0))
                      .where(
                          (F.col("code").rlike("[a-z][0-9][0-9]")) | (F.col("is_CKD") == 1)
                      )
                      .withColumn("code",
                                  F.when(
                                      F.col("is_CKD") == 1, F.lit("n18")
                                  )
                                  .otherwise(F.col("code"))
                                 )
                      .distinct()
                      .drop("Description","is_CKD")
                     )
display(diagnosis_df_codes)

# COMMAND ----------


