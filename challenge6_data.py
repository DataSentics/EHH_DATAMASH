# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# COMMAND ----------

labs_data_df = spark.read.table('labs_all')
display(labs_data_df)

# COMMAND ----------

NCLP_df = labs_data_df.filter(F.col('NCLP').isNull()).groupBy('Analyte').count()

# COMMAND ----------

NCLP_df.count()

# COMMAND ----------

code_df = labs_data_df.filter(F.col('Code').isNull()).groupBy('Analyte').count()

# COMMAND ----------

code_df.count()

# COMMAND ----------

NCLP_df.join(code_df, on='Analyte', how='outer').display()

# COMMAND ----------

labs_data_df.filter(F.col('Analyte')=='s_anti-tTG IgA').display()

# COMMAND ----------

labs_data_df.groupBy('Patient','EntryDate','NCLP','Report').count().filter(F.col('count')>1).display()

# COMMAND ----------

labs_data_df.filter(F.col('Patient')==195042).filter(F.col('EntryDate')=='2022-05-05').filter(F.col('NCLP')==16371).display()


# COMMAND ----------

labs_data_df.withColumn('Time', F.col('EntryTime').cast('int')).display()

# COMMAND ----------

labs_data_df.select('NCLP','Analyte').filter(F.col('NCLP')==6879).dropDuplicates().display()

# COMMAND ----------

labs_data_df.select('Analyte','NCLP','Code').drop_duplicates().display()

# COMMAND ----------

pivotDF = labs_data_df.pivot("Country")

# COMMAND ----------

display(labs_data_df.select("Patient").distinct())

# COMMAND ----------

diagnosis_df = spark.read.table('dg_from_report')
display(diagnosis_df)

# COMMAND ----------

diagnosis_df = (diagnosis_df.withColumn('main_dg_code', F.regexp_extract('mainDgCode', '(.*)(\:)(.*)',1))
                            .withColumn('main_dg_description', F.regexp_extract('mainDgCode', '(.*)(\:)(.*)',3))
                            .withColumn('main_dg_description', F.when(F.col('mainDgCode'))
                                       )
                            .withColumn('other_dg_code', F.regexp_extract('OtherDgCode', '(.*)(\:)(.*)',1))
                            .withColumn('other_dg_description', F.regexp_extract('OtherDgCode', '(.*)(\:)(.*)',3))
               )

# COMMAND ----------

diagnosis_df.display()

# COMMAND ----------

# quick test
display(labs_data_df.filter(col("Patient")==1191501))
display(diagnosis_df.filter(col("Patient")==1191501))

# COMMAND ----------


