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
                      .drop("Description","is_CKD")
                      .distinct()
                     )
display(diagnosis_df_codes)

# COMMAND ----------

diagnosis_df_codes.count()

# COMMAND ----------

codes_corresponding_descriptions_df = (diagnosis_df
                                       .withColumn('code', F.substring(F.col('mainDgCode'), 0,3))
                                       .withColumn('description', F.split(F.col('mainDgCode'), ': ').getItem(1))
                                       .drop("Patient","Date","mainDgCode","OtherDgCode")
                                       .distinct()
                                      )
display(codes_corresponding_descriptions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##MKN-10 convertor

# COMMAND ----------

mkn_10_df = spark.read.table('mkn_10').selectExpr("KodSTeckou as Code", "Nazev as Description", "NazevPlny as DescriptionExt")
display(mkn_10_df)

# COMMAND ----------

mkn_10_joint_df = (mkn_10_df
                    .select("Code", "Description")
                   ).union(
                    mkn_10_df.selectExpr("Code", "DescriptionExt as Description")
                   ).union(
                    codes_corresponding_descriptions_df
                   )
mkn_10_joint_df = (mkn_10_joint_df
                   .withColumn('code', F.substring(F.col('code'), 0,3))
                   .distinct()
                  )
display(mkn_10_joint_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join unpaired descriptions to codes

# COMMAND ----------

diagnosis_other_df = (diagnosis_df
                      .withColumn("Year", F.year("Date"))
                      .selectExpr("Patient", "Year", "OtherDgCode as Description")
                      .join(mkn_10_joint_df, on= "Description")
                      .select("Patient","Year","code")
                      .distinct()
                     )
display(diagnosis_other_df)

# COMMAND ----------

diagnosis_other_df.count()

# COMMAND ----------

# def matchCode(full_desc):
#     code = [mkn_10_both_pd["code"][i] for i, description in enumerate(mkn_10_both_pd["Description"]) if description in full_desc]
#     if len(code) > 0:
#         print(code)
#         return code[0]
#     elif len(code) == 1:
#         return str(code[0])
#     else:
#         return "NA"

# matchUDF = udf(lambda x: matchCode(x))

# diagnosis_matched_df = (diagnosis_other_df
#                         .limit(5)
#                         .withColumn("matchedCode", matchUDF(F.col("Description")))
#                        )
# display(diagnosis_matched_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## output df

# COMMAND ----------

diagnosis_output_df = (diagnosis_df_codes
                       .union(diagnosis_other_df)
                       .distinct()
                      )
print(diagnosis_output_df.count())
display(diagnosis_output_df)

# COMMAND ----------

print(diagnosis_output_df.where("code=='n18'").select('Patient').drop_duplicates().count())

# COMMAND ----------

diagnosis_output_df.write.saveAsTable("diagnoses")

# COMMAND ----------


