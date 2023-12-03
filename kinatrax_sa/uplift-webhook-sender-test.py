# Databricks notebook source
# MAGIC %pip install Flask

# COMMAND ----------

import requests
import json

webhook_url = 'https://296f-35-155-15-56.ngrok.io/uplift-webhook-receiver-test'
data = {'key1': 'value1', 'key2': 'value2'}

response = requests.post(webhook_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})

print(response.status_code)
if response.status_code == 200:
    print('Webhook sent successfully')
else:
    print('Error sending webhook')

# COMMAND ----------


