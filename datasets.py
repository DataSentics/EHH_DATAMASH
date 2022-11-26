# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

bmi_df = spark.read.table('bmi_dataset')
sex_df = spark.read.table('hack_sex')
lab_tests_df = spark.read.table('pivot_lab_data').drop('20042', '20411') #droping weight and height
diagnosis_df = spark.read.table('dg_from_report')
diagnoses = spark.read.table('diagnoses')

# COMMAND ----------

w = Window.orderBy("date").partitionBy('Patient')
bmi_df = (bmi_df.withColumn('date_lagged', F.lag('date').over(w))
              .withColumn('date_lagged', F.when(F.col('date_lagged').isNull(), F.col('date')).otherwise(F.col('date_lagged')))
              .drop('Weight', 'Height')
         )

# COMMAND ----------

diagnose_to_train_on = ['n18']
diagnoses_2022 = (diagnoses.filter(F.col('Year')==2022)
                  .withColumn('CKD', F.when(F.col('code').isin(diagnose_to_train_on),1)
                   .otherwise(0))
                   .drop('code','Year')     
                 )

# COMMAND ----------

df = diagnoses_2022.join(lab_tests_df, on='Patient').join(sex_df, on='Patient').join(bmi_df, 'Patient')
df = (df.filter(F.col('Entry')>=F.col('date_lagged'))
        .filter(F.col('Entry')<F.col('date'))
     )

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.saveAsTable('labeled_dataset')

# COMMAND ----------

diagnosis_df.display()
lab_tests_df.display()
sex_df.display()
bmi_df.display()

# COMMAND ----------



# COMMAND ----------

#Albumin 11447, 509, 507
#GFR 17339
labs_data_orig=spark.read.table('labs_all')

# COMMAND ----------

#NCLP high risk patients
#11447 >3g/mol

display(labs_data_orig
 #.filter(F.col('NCLP')==17339)
 .filter(F.col('NCLP').isin([509,11447]))
 #.filter(F.col('Analyte').contains('Albumin'))
 .withColumn('Value', F.col('ValueNumber').cast('Float'))
# .withColumn('Value_adj', F.when(F.col('Unit')=='mg/l', F.col('Value'))
            
           # )
 #.filter(F.col('Unit').isin(['ml/s/spt','ml/s/1,73 m2']))
# .groupBy('Unit','NCLP').agg(F.max('Value'), F.min('Value'))
)

# COMMAND ----------

lab_tests_df.display()

# COMMAND ----------

# DBTITLE 1,code to detect CKD undiagnosed patients
lab_tests_df.filter((F.col('17339')
                    
                    )

# COMMAND ----------

bsa_calc = bmi_df.withColumn('BSA', F.sqrt((F.col('Height')*F.col('Weight'))/3600))

# COMMAND ----------

bsa_calc.display()

# COMMAND ----------

NCLP_patients = (labs_data_orig
 .filter(F.col('NCLP')==17339)
 #.filter(F.col('NCLP').isin([11447, 509, 507]))
 #.filter(F.col('Analyte').contains('Albumin'))
 .withColumn('Value', F.col('ValueNumber').cast('Float'))
 .filter(F.col('Unit').isin(['ml/s/spt','ml/s/1,73 m2']))
                )
bsa_calc = bmi_df.withColumn('BSA', F.sqrt((F.col('Height')*F.col('Weight'))/3600))
bsa_calc = bsa_calc.groupBy('Patient').agg(F.avg('BSA').alias('BSA')).dropna()
df = NCLP_patients.join(bsa_calc, on='Patient', how='inner')

NCLP_df = (df.withColumn('convert_GFR', F.col('BSA')/1.73)
          .withColumn('convert_GFR',F.coalesce(F.col('convert_GFR'),F.lit(1.1666)))
          .withColumn('Value_adj', F.col('Value')*F.col('convert_GFR'))
     )

# COMMAND ----------

NCLP_df = NCLP_df.withColumn('CDR_risk', F.when(F.col('Value_adj')<=0.44, 1)
                             .otherwise(F.when(F.col('Value_adj')<0.60, 0.5)
                             .otherwise(0)))

# COMMAND ----------

NCLP_df.write.saveAsTable('NCLP_patients')

# COMMAND ----------

#prepare final table


# COMMAND ----------

diagnoses.withColumn()

# COMMAND ----------

lab_tests_df.display()

# COMMAND ----------

lab_test_df.withColumn()
