# Databricks notebook source
# MAGIC %md
# MAGIC #Parameters

# COMMAND ----------

dbutils.widgets.text("source_folder", "netflix_directors")
dbutils.widgets.text("target_folder", "netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC #Variables

# COMMAND ----------

var_src_folder = dbutils.widgets.get("source_folder")
var_tgt_folder = dbutils.widgets.get("target_folder")

# COMMAND ----------

bronze = "abfss://bronze@dlsnetflixprojectstorage.dfs.core.windows.net"
silver = "abfss://silver@dlsnetflixprojectstorage.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading Data

# COMMAND ----------

dframe = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"{bronze}/{var_src_folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Displaying Data

# COMMAND ----------

dframe.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Writing Data To Silver

# COMMAND ----------

dframe.write\
.mode("overwrite")\
.format("delta")\
.save(f"{silver}/{var_tgt_folder}")