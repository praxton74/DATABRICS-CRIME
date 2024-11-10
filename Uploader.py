# Databricks notebook source
container_name =
account_name =
mount_point =

# COMMAND ----------

application_id = dbutils.secrets.get(scope='', key='')
tenant_id = dbutils.secrets.get(scope='', key='')
secret = dbutils.secrets.get(scope='', key='')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/'
