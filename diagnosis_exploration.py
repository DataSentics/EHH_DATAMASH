# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosis

# COMMAND ----------

diagnosis_df = spark.read.table('dg_from_report')
display(diagnosis_df)

# COMMAND ----------

print(f"Počet řádků: {diagnosis_df.count()}")
print(f"Počet pacientů: {diagnosis_df.select('Patient').distinct().count()}")
print(f"Počet různých mainDG: {diagnosis_df.select('mainDgCode').distinct().count()}")
print(f"Počet různých otherDG: {diagnosis_df.select('OtherDgCode').distinct().count()}")
display(diagnosis_df.summary().collect()[0])

# COMMAND ----------

display(diagnosis_df
        .withColumn("year", F.year(F.col("Date")))
        .groupBy("year")
        .count()
        .orderBy("year")
       )
display(diagnosis_df
        .groupby("Date")
        .count()
       )

# COMMAND ----------

display(diagnosis_df.where("Patient==8451").orderBy("date"))

# COMMAND ----------

display(diagnosis_df
        .withColumn("Year", F.year(F.col("Date")))
        .groupBy("Patient", "Year")
        .agg(
            F.count("mainDgCode").alias("n_events")
        )
        .groupBy("n_events", "Year")
        .count()
        .orderBy("n_events")
       )

# COMMAND ----------

diagnosis_df_splitted = (diagnosis_df
                         .withColumn('main_code', F.split(diagnosis_df['mainDgCode'], ': ').getItem(0))
                         .withColumn('description', F.split(diagnosis_df['mainDgCode'], ': ').getItem(1))
                        )

n_codes = (diagnosis_df_splitted
           .groupBy("mainDgCode","main_code")
           .agg(
               F.count("Patient").alias("n_codes")
           )
          )
n_descriptions = (diagnosis_df_splitted
                  .groupBy("mainDgCode","description")
                  .agg(
                      F.count("Patient").alias("n_descriptions")
                  )
                 )

display(n_codes
        .join(n_descriptions, on = "mainDgCode",how = "outer")
        .where("n_codes!=n_descriptions")
       )

# COMMAND ----------

display(diagnosis_df_splitted
        .where("description==OtherDgCode")
       )
diagnosis_df_alt = diagnosis_df.selectExpr("Patient", "Date", "mainDgCode", "OtherDgCode as description")
display(diagnosis_df_splitted
        .select("Patient","Date","mainDgCode","description")
        .distinct()
        .join(diagnosis_df_alt, on = ["Patient", "Date", "description"])
       )

# COMMAND ----------

diagnosis_df_splitteddisplay(diagnosis_df_alt.where("Patient==78266").where("mainDgCode=='J45.8: Smíšené astma, lehké intermitentní až lehké perzistující'").where("Date=='2015-01-20'"))
display(diagnosis_df_splitted.where("Patient==78266").where("mainDgCode=='J45.8: Smíšené astma, lehké intermitentní až lehké perzistující'").where("Date=='2015-01-20'"))

# COMMAND ----------

diagnosis_df_codes = (diagnosis_df
                      .withColumn("Year", F.year("Date"))
                      .select("Patient", "Year", "mainDgCode","OtherDgCode")
                      .distinct()
                      .withColumn('code', F.substring(diagnosis_df['mainDgCode'], 0,3))
                      .withColumn('code_short', F.substring(diagnosis_df['mainDgCode'], 0,1))
                      .withColumn("is_N", F.when(F.col("code_short") == "N", 1).otherwise(0))
                      .withColumn("is_CKD", F.when(F.col("code") == "N18", 1).otherwise(0))
                      .where("is_CKD==1")
#                       .groupBy("is_CKD")
#                       .count()
                     )
display(diagnosis_df_codes)

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
                          (F.col("code") == "n18") |
                          F.col("Description").contains('chronické onemocnění ledvin') |
                          F.col("Description").contains("ckd")
                          , 1).otherwise(0))
#                       .where("is_CKD==1")
#                       .withColumn('code', F.substring(F.col('OtherDgCode'), 0,3))
#                       .withColumn('code_short', F.substring(F.col('OtherDgCode'), 0,1))
                      .groupBy("Patient","Year")
                      .agg(
                          F.sum("is_CKD").alias("is_CKD")
                      )
                      .withColumn("is_CKD", F.when(
                          F.col("is_CKD") > 0, 1
                         ).otherwise(0)
                       )
                      .groupBy("Year","is_CKD")
                      .agg(
                          F.count("Patient").alias("num_patients")
                      )
                     )
display(diagnosis_df_codes)

# COMMAND ----------

data = [("chronic kidney failure", "N18")]
rdd = spark.sparkContext.parallelize(data)
columns = ["Name", "Code"]
ckd_df = rdd.toDF(columns)

relevant_diseases = (spark.read.table("relevant_diseases")
#                      .union(ckd_df)
                     .withColumn("Code", F.lower(F.col("Code")))
                    )
display(relevant_diseases)
diagnoses = spark.read.table("diagnoses")
display(diagnoses)

# COMMAND ----------

diagnoses.select("Patient").distinct().count()

# COMMAND ----------

patients_with_relevant_diseases = (diagnoses
                                   .drop("Year")
                                   .distinct()
                                   .join(relevant_diseases, on = "code", how = "outer")
                                   .withColumn("is_CKD", F.when(F.col("code") == "n18", 1).otherwise(0))
                                   .withColumn("is_diabetese_II", F.when(F.col("code") == "e11", 1).otherwise(0))
                                   .withColumn("relevant", F.when(F.col("Name").isNotNull(), 1).otherwise(0))
                                   .groupBy("Patient")
                                   .agg(
                                       F.sum("is_CKD").alias("is_CKD"),
                                       F.sum("is_diabetese_II").alias("is_diabetese_II"),
                                       F.sum("relevant").alias("relevant")
                                   )
                                   .withColumn("relevant", F.when(F.col("relevant") > 0, 1).otherwise(0))
#                                    .where("is_CKD==")
                                   .groupBy("is_CKD","is_diabetese_II")
                                   .agg(
                                       F.count("Patient")
                                   )
#                                    .select("other_than_CKD")
#                                    .where(F.col("is_CKD").isNull())
#                                    .count()
                                  )
display(patients_with_relevant_diseases)

# COMMAND ----------


