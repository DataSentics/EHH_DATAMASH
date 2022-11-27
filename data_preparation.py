# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pyspark


# COMMAND ----------

def preprocess_lab_data(labs_data_df: pyspark.sql.DataFrame):
    return (labs_data_df
            .withColumn("Value", F.col("ValueNumber").cast('double'))
            #prepate timestamp to standardized format
            .withColumn("Time", F.to_timestamp("EntryTime", 'dd.MM.yyyy H:mm:ss'))
            .withColumn("TimeString", F.date_format(F.col('Time'), 'hh:mm:ss'))
            .withColumn("DateString", F.date_format(F.col('EntryDate'), 'dd-MM-yyyy'))
            .withColumn("Entry", F.to_timestamp(F.concat(F.col("DateString"), F.lit(' '),F.col("TimeString")), 'dd-MM-yyyy hh:mm:ss'))
            .select(["Patient", "Entry", "NCLP", "Value"])
            #weight and height are added to bmi dataset and droped from lab dataset
            .filter(~F.col('NCLP').isin([20042, 20411]))
            .dropna()
           )
    
def pivot_lab_data(preprocessed_lab_dataset: pyspark.sql.DataFrame):
    return (preprocessed_lab_dataset
     .groupBy('Patient','Entry')
     .pivot('NCLP')
     .agg(F.min('Value'))
    )

def enrich_bmi(lab_data: pyspark.sql.DataFrame, bmi_data: pyspark.sql.DataFrame):
    enrich_df = (lab_data.filter(F.col('NCLP').isin([20042, 20411]))
            .groupBy('Patient','EntryDate','EntryTime')
            .pivot('Analyte')
            .agg(F.max('ValueNumber'))
            .withColumn('Height', F.coalesce(F.col('výška'), F.col('výška pacienta')))
            .withColumn('Weight', F.coalesce(F.col('váha pacienta'), F.col('Hmotnost')))
            .withColumn('BMI', F.col('Weight')/(F.col('Height')/100))
            .select('Patient', F.col('EntryDate').alias('date'), 'BMI', 'Weight', 'Height')
            .dropna())
    
    height_data = (bmi_data
       .groupby('Patient')
       .agg(F.max('Height')
       .alias('Height'))
       .dropna()
      )
    bmi_data = (bmi_data.drop('Height')
                .join(height_data, 'Patient', how='left')
                .withColumnRenamed('Weight (kg)','Weight')
                
               )
    bmi_data = bmi_data.unionByName(enrich_df)
    w = Window.orderBy("date").partitionBy('Patient')
    bmi_df = (bmi_data.withColumn('date_lagged', F.lag('date').over(w))
              .withColumn('date_lagged',
                          F.when(F.col('date_lagged').isNull(), F.col('date'))
                           .otherwise(F.col('date_lagged'))
                         )
             )
    bmi_df = bmi_df.withColumn('Height',F.when(F.col('Height').isNull(), F.sqrt(F.col('Weight')/F.col('BMI'))).otherwise(F.col('Height')))
    return bmi_df

def diagnosis_2022_prep(diagnosis: pyspark.sql.DataFrame, diagnose_to_train_on: list = ['n18'] ):
    diagnosis_2022 = (diagnosis.filter(F.col('Year')==2022)
                  .withColumn('CKD', F.when(F.col('code').isin(diagnose_to_train_on),1)
                   .otherwise(0))
                   .drop('code','Year')     
                 )
    return (diagnoses_2022
             .groupBy('Patient')
             .agg(F.max('CKD').alias('CKD'))
            )

def join_datasets(lab_tests: pyspark.sql.DataFrame, diagnosis_2022: pyspark.sql.DataFrame, sex_df: pyspark.sql.DataFrame, bmi_df: pyspark.sql.DataFrame):
    df = (lab_tests_df
                    .join(diagnosis_2022, on='Patient', how='left')
                    .join(sex_df, on='Patient', how='left')
                    .join(bmi_df, 'Patient', how='left')
                   )
    return (df
               .filter(((F.col('Entry')>=F.col('date_lagged'))&(F.col('Entry')<F.col('date')))|((F.col('Entry')>=F.col('date'))&(F.col('date')==F.col('date_lagged'))))
              .filter(F.col('Patient').isNotNull()).filter(F.col('Entry').isNotNull())
                  )

def GFR_data_preprocessor(labs_data: pyspark.sql.DataFrame, bmi_dataset: pyspark.sql.DataFrame):
        NCLP_patients = (labs_data
                 .filter(F.col('NCLP')==17339)
                 .withColumn('Value', F.col('ValueNumber').cast('Float'))
                 #prepate timestamp to standardized format
                 .withColumn("Time", F.to_timestamp("EntryTime", 'dd.MM.yyyy H:mm:ss'))
                 .withColumn("TimeString", F.date_format(F.col('Time'), 'hh:mm:ss'))
                 .withColumn("DateString", F.date_format(F.col('EntryDate'), 'dd-MM-yyyy'))
                 .withColumn("Entry", F.to_timestamp(F.concat(F.col("DateString"), F.lit(' '),F.col("TimeString")), 'dd-MM-yyyy hh:mm:ss'))
                 .filter(F.col('Unit').isin(['ml/s/spt','ml/s/1,73 m2']))
                )
        bsa_calc = (bmi_dataset
                          .withColumn('BSA', 0.007184*F.pow(F.col('Height'), F.lit(0.725))*F.pow(F.col('Weight'),F.lit(0.425)))
                          .groupBy('Patient')
                          .agg(F.max('BSA').alias('BSA'))
                          .dropna()
                         )
        df = (NCLP_patients
               .join(bsa_calc, on='Patient', how='inner')
              .withColumn('convert_GFR', F.col('BSA')/1.73)
              .withColumn('convert_GFR',F.coalesce(F.col('convert_GFR'),F.lit(1.1666)))
              .withColumn('Value_adj', F.col('Value')*60)
              .withColumn('Value_adj', F.when(F.col('Value_adj')>100, F.lit(100)).otherwise(F.col('Value_adj')))
              .withColumn("Time", F.to_timestamp("EntryTime", 'dd.MM.yyyy H:mm:ss'))
              .withColumn("TimeString", F.date_format(F.col('Time'), 'hh:mm:ss'))
              .withColumn("DateString", F.date_format(F.col('EntryDate'), 'dd-MM-yyyy'))
              .withColumn("Entry", F.to_timestamp(F.concat(F.col("DateString"), F.lit(' '),F.col("TimeString")), 'dd-MM-yyyy hh:mm:ss'))
                   )
        return (df.withColumn('CDR_risk', F.when(F.col('Value_adj')<=44, 1)
                             .otherwise(F.when(F.col('Value_adj')<60, 0.5)
                             .otherwise(0)))
                  )

def prepare_diagnosis(diagnosis: pyspark.sql.DataFrame):
        diagnosis_main_df = (diagnosis_df
                     .withColumn("Year", F.year("Date"))
                     .selectExpr("Patient", "Year", "mainDgCode as Description")
                    )
        diagnosis_other_df = (diagnosis_df
                      .withColumn("Year", F.year("Date"))
                      .selectExpr("Patient", "Year", "OtherDgCode as Description")
                     )
        diagnosis_joint_df = diagnosis_main_df.union(diagnosis_other_df)
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
        return diagnosis_df_codes
              

# COMMAND ----------

# DBTITLE 1,Labs data preprocessing
labs_data_df = spark.read.table('labs_all')
bmi_df = spark.read.table('bmi_dataset')
sex_df = spark.read.table('hack_sex')
lab_tests_df = spark.read.table('pivot_lab_data').drop('20042', '20411') #droping weight and height
diagnosis_df = spark.read.table('dg_from_report')
diagnoses = spark.read.table('diagnoses')
labeled_dataset = spark.read.table('labeled_dataset')

bmi_df = enrich_bmi(labs_data_df, bmi_df)
GFR_data = GFR_data_preprocessor(labs_data_df, bmi_df)

# COMMAND ----------

print('Diagnosed patients in our dataset:', diagnosis_df.select('Patient').drop_duplicates().count(), '\n',
    'Patients in full dataset:',labs_data_df.select('Patient').drop_duplicates().count(), '\n'
      'Patients with GFR test(NCLP=17339):', labs_data_df.filter(F.col('NCLP')==17339).select('Patient').drop_duplicates().count(), '\n'
      'Ratio of patients with GFR:', labs_data_df.filter(F.col('NCLP')==17339).select('Patient').drop_duplicates().count()/labs_data_df.select('Patient').drop_duplicates().count(), '\n'  
     )

# COMMAND ----------

data = [("chronic kidney failure", "N18")]
rdd = spark.sparkContext.parallelize(data)
columns = ["Name", "Code"]
ckd_df = rdd.toDF(columns)

relevant_diseases = spark.read.table("relevant_diseases").union(ckd_df).withColumn("Code", F.lower(F.col("Code")))
display(relevant_diseases.groupBy('Name').agg(F.collect_list('Code').alias('List of international codes')))

# COMMAND ----------

n18_df = diagnoses.filter(F.col('code')=='n18').select('Patient', F.lit(1).alias('was_diagnosed')).drop_duplicates()

# COMMAND ----------

joined_df = n18_df.join(positive_found_labs, on='Patient', how='full')

# COMMAND ----------

joined_df.fillna(0).groupBy('was_diagnosed','CDR_risk').count().display()

# COMMAND ----------


