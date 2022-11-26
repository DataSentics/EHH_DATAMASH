# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.model_selection import train_test_split
import shap
import random
from catboost import CatBoostRegressor, Pool, EShapCalcType, EFeaturesSelectionAlgorithm

# COMMAND ----------

df_labeled = spark.read.table(
  'hive_metastore.default.labeled_dataset'
)

df_nclp_patients = spark.read.table(
  'hive_metastore.default.nclp_patients'
)

df_naming = spark.read.table(
  'hive_metastore.default.nclp_naming'
)

# COMMAND ----------

display(df_nclp_patients.select("Value_adj", "Value"))

# COMMAND ----------

df_labeled# main table for training
df_tested_patients = (
    df_nclp_patients
    .withColumnRenamed("Value_adj", "target")
    .withColumnRenamed("Patient", "TestedPatient")
    .withColumnRenamed("Entry", "GEntry")
    .select("TestedPatient", "GEntry", "target")
)

df_joined = (
    # patients with G test
    df_tested_patients
    # join historical lab tests
    .join(df_labeled, (df_tested_patients.TestedPatient ==  df_labeled.Patient) & (df_tested_patients.GEntry >=  df_labeled.Entry), "left")
    .drop("Patient", "date_lagged", "date")
    .withColumnRenamed("TestedPatient", "Patient")
)

df_joined.cache()

# COMMAND ----------

# use only patients with known Sex
df_joined = df_joined.filter(F.col("Sex").isNotNull())

# COMMAND ----------

# the main test: 17339 is removed
# 17341, "14845", "18066" - GFR
# 8574, 8572 - kreatins
main_tests = {"17339", "17341", "14845", "18066", "8574", "8572"}
not_features = {"Patient", "target", "GEntry", "Entry", "CKD", "Weight", "Height"}
feature_cols = list(set(list(df_joined.columns)) - not_features - main_tests)
feature_cols.sort()

# COMMAND ----------

w  = Window.partitionBy("Patient", "GEntry").orderBy(F.col("Entry").desc())

# COMMAND ----------

df_latest = (
    df_joined
    .select(["Patient", "GEntry", "target"] + [(F.first(F.col(x), ignorenulls=True).over(w)).alias(x) for x in feature_cols])
    .groupBy("Patient", "GEntry", "target")
    .agg(
        *[(F.first(F.col(x), ignorenulls=True)).alias(x) for x in feature_cols]
    )
)

# COMMAND ----------

pdf = df_latest.toPandas()

# COMMAND ----------

X = pdf.drop(["target", "GEntry", "Patient"], axis=1)
y = pdf["target"]

# COMMAND ----------

X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.20, random_state=42)

# COMMAND ----------

pdf_naming = df_naming.toPandas()

def name_NCLP(feature_cols):
    named_feature_cols = []
    for f in feature_cols:
        try:
            named_feature_cols.append(str(f) + "-" + pdf_naming[pdf_naming["NCLP"]==int(f)]["NCLP_name"].values[0])
        except:
            named_feature_cols.append(f)
    return named_feature_cols

# COMMAND ----------

model = CatBoostRegressor()

# Fit model
model.fit(
    X_train,
    y_train,
    cat_features=["Sex"],
    eval_set=(X_val, y_val)
)

# COMMAND ----------

model.set_feature_names(named_feature_cols)

# COMMAND ----------

X.columns = named_feature_cols

# COMMAND ----------

explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X)

# COMMAND ----------

shap.summary_plot(shap_values, X)

# COMMAND ----------

r = random.randint(0, X.shape[0])

shap.force_plot(
    explainer.expected_value, 
    shap_values[r,:], 
    X.iloc[r,:], 
    matplotlib=True
) 

# COMMAND ----------

train_pool = Pool(X_train, y_train, cat_features=["Sex"], feature_names=feature_cols)
test_pool = Pool(X_val, y_val, cat_features=["Sex"], feature_names=feature_cols)

model = CatBoostRegressor(iterations=1000, random_seed=0)
summary = model.select_features(
    train_pool,
    eval_set=test_pool,
    features_for_select=f'0-{len(feature_cols)-1}',
    num_features_to_select=200,
    steps=20,
    algorithm=EFeaturesSelectionAlgorithm.RecursiveByShapValues,
    shap_calc_type=EShapCalcType.Regular,
    train_final_model=True,
    logging_level='Silent',
    plot=True
)
