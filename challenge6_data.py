# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

labs_data_df = spark.read.table('labs_all')
display(labs_data_df)

# COMMAND ----------

transformed_labs_data_df = (
    labs_data_df
    .withColumn("Value", F.coalesce(F.col("ValueNumber"), F.col("ValueText")))
    .withColumn("Time", F.to_timestamp("EntryTime", 'dd.MM.yyyy H:mm:ss'))
    .withColumn("TimeString", F.date_format(F.col('Time'), 'hh:mm:ss'))
    .withColumn("DateString", F.date_format(F.col('EntryDate'), 'dd-MM-yyyy'))
    .withColumn("Entry", F.to_timestamp(F.concat(F.col("DateString"), F.lit(' '),F.col("TimeString")), 'dd-MM-yyyy hh:mm:ss'))
    .select(["Patient", "Entry", "NCLP", "Value"])
    .dropna()
)

# COMMAND ----------

transformed_labs_data_df = transformed_labs_data_df.filter(~F.col('NCLP').isin([20042, 20411]))

# COMMAND ----------

transformed_labs_data_df = transformed_labs_data_df.groupBy('Patient','Entry').pivot('NCLP').agg(F.min('Value'))

# COMMAND ----------

transformed_labs_data_df.write.option('overwriteSchema',True).mode('overwrite').saveAsTable('pivot_lab_data')

# COMMAND ----------

labs_data_df.filter(F.col('NCLP').isNotNull()).select('Analyte','NCLP').dropDuplicates().filter(F.col('Analyte').contains('výška')).display()

# COMMAND ----------

enrich_bmi_data = labs_data_df.filter(F.col('NCLP').isin([20042, 20411]))

# COMMAND ----------

bmi_data.display()

# COMMAND ----------

#20042, 20411 == NCLP for weight and height
enrich_bmi_data = (enrich_bmi_data
                   .groupBy('Patient','EntryDate','EntryTime')
                   .pivot('Analyte')
                   .agg(F.max('ValueNumber'))
                   .withColumn('Height', F.coalesce(F.col('výška'), F.col('výška pacienta')))
                   .withColumn('Weight', F.coalesce(F.col('váha pacienta'), F.col('Hmotnost')))
                  )

# COMMAND ----------

enrich_bmi_data = enrich_bmi_data.withColumn('BMI', F.col('Weight')/(F.col('Height')/100)).select('Patient', F.col('EntryDate').alias('date'), 'BMI', 'Weight', 'Height')

# COMMAND ----------

enrich_bmi_data = enrich_bmi_data.dropna()

# COMMAND ----------

bmi_data = bmi_data.union(enrich_bmi_data)

# COMMAND ----------

pivot_table = transformed_labs_data_df.groupBy('Patient','EntryDate','EntryTime').pivot('NCLP').agg(F.min('Value'))

# COMMAND ----------

display(pivot_table)

# COMMAND ----------

display(pivot_table.groupBy('Patient').count())

# COMMAND ----------

pivot_table.select('Patient').drop_duplicates().count()

# COMMAND ----------

pivot_table.write.mode('overwrite').saveAsTable('pivot_lab_data')

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

bmi_data = spark.read.table('bmi_dataset')

# COMMAND ----------

bmi_data.display()

# COMMAND ----------

grouped_data = bmi_data.groupby('Patient').agg(F.max('Height (cm)').alias('Height')).dropna()

# COMMAND ----------

bmi_data = bmi_data.join(grouped_data, 'Patient', how='left')

# COMMAND ----------

bmi_data = bmi_data.drop('Height (cm)')

# COMMAND ----------

bmi_data.write.option('overwriteSchema',True).mode('overwrite').saveAsTable('bmi_dataset')

# COMMAND ----------

bmi_data= spark.read.table('bmi_dataset')

# COMMAND ----------

bmi_data = bmi_data.withColumnRenamed('Weight (kg)','Weight')

# COMMAND ----------


