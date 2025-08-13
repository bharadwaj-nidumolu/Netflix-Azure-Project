# Databricks notebook source
bronze = "abfss://bronze@dlsnetflixprojectstorage.dfs.core.windows.net"
silver = "abfss://silver@dlsnetflixprojectstorage.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data
# MAGIC

# COMMAND ----------

df = spark.read\
        .format("delta")\
        .option("header", True)\
        .option("inferSchema", True)\
        .load(f"{bronze}/netflix_titles")

# COMMAND ----------

# MAGIC %md
# MAGIC # Printing Data

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tranformation

# COMMAND ----------

from pyspark.sql.functions import col, split, when, dense_rank, count
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

# COMMAND ----------

# Print the schema
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC All columns are string type because by default csv stores data in string type

# COMMAND ----------

# Converting type String to Integer
df = df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))\
    .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Creating new columns (shortTitle) by splitting the title column
df = df.withColumn("shortTitle",split(col("title"),":")[0])

# COMMAND ----------

display(df)

# COMMAND ----------

# creating new column shortRating by splitting the rating column
df = df.withColumn("shortRating",split(col("rating"),"-")[0])
display(df)

# COMMAND ----------

# Creating new column typeFlag
df = df.withColumn("typeFlag", when(col("type")=="Movie",1)\
                        .when(col("type")=="TV Show",2)\
                        .otherwise(0))

# COMMAND ----------

display(df)

# COMMAND ----------

# Creating a new column duration _ranking using window functions on duration
df = df.withColumn("duration_ranking", dense_rank().over(Window.orderBy(col("duration_minutes").desc())))
display(df)

# COMMAND ----------

# Total Count on type : "Movie" or "TV Show"
dfCount = df.groupBy("type").agg(count("*").alias("Total Count"))

# COMMAND ----------

# Creating temp view
dfCount.createOrReplaceTempView("dfCountView")
dfTemp = spark.sql("select * from dfCountView")
display(dfTemp)

# COMMAND ----------

display(dfCount)

# COMMAND ----------

# MAGIC %md
# MAGIC #Writing data into Silver Layer

# COMMAND ----------

display(df)

# COMMAND ----------

df.write\
    .format("delta")\
    .mode("overwrite")\
    .option("path", f"{silver}/netflix_titles" )\
    .save()