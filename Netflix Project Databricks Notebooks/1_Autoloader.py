# Databricks notebook source
# MAGIC %md
# MAGIC # Autoloader

# COMMAND ----------

checkpoint_location = "abfss://schemalocation@dlsnetflixprojectstorage.dfs.core.windows.net/checkpoints"
raw = "abfss://raw@dlsnetflixprojectstorage.dfs.core.windows.net"
bronze = "abfss://bronze@dlsnetflixprojectstorage.dfs.core.windows.net"
silver = "abfss://silver@dlsnetflixprojectstorage.dfs.core.windows.net"
gold = "abfss://gold@dlsnetflixprojectstorage.dfs.core.windows.net"



# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation", checkpoint_location)\
  .load(raw)

# COMMAND ----------

display(df)

# COMMAND ----------

df.writeStream\
.format("csv")\
.option("path", bronze)
.option("checkpointLocation", checkpoint_location)\
.trigger(processingTime="10 seconds")\
.start(f"{bronze}/netflix_titles")