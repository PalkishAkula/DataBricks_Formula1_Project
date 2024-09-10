# Databricks notebook source
client_id="8c5282d0-b297-4f8b-bf0b-3370c84e34d2"
tenant_id="12d2579e-bf95-4ebe-8f76-c8ad1f17d285"
client_secret="dVO8Q~Oz~rfuHN65jeSL3VuvuUNjnfeQuzAlhcDV"

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.formula1dlgen2.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlgen2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlgen2.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlgen2.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlgen2.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlgen2.dfs.core.windows.net"))

# COMMAND ----------

#mount
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlgen2.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlgen2/demo",
  extra_configs = configs)


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@formula1dlgen2.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlgen2/raw",
  extra_configs = configs)


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://presentation@formula1dlgen2.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlgen2/presentation",
  extra_configs = configs)


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@formula1dlgen2.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlgen2/processed",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dlgen2/raw")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlgen2/raw"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlgen2/demo/circuits.csv"))