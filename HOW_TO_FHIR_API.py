# Databricks notebook source
# MAGIC %md
# MAGIC ### Python api do get data
# MAGIC <https://openexchange.intersystems.com/package/fhir-client-python>

# COMMAND ----------

from fhirpy import SyncFHIRClient
from fhir.resources.patient import Patient

from fhir.resources.observation import Observation

from fhir.resources.humanname import HumanName

from fhir.resources.contactpoint import ContactPoint


import json

# COMMAND ----------

url ='https://fhir.57r7yz738q3b.static-test-account.isccloud.io/Patient?address=Mirova'
key = '4xBy4WHJDr25XFi8DaYpi2fEIGoB2OVN2JdCHsur'


# COMMAND ----------

client = SyncFHIRClient(url=url, extra_headers={"x-api-key":key})

# COMMAND ----------

patients_resources = client.resources('Patient')
patients_resources.fetch_all()
#patient0 = Patient.parse_obj(patients_resources.search(family='Novotna').first())

# COMMAND ----------



# COMMAND ----------


