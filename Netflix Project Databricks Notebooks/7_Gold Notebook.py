# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Notebook - GOLD Layer

# COMMAND ----------

silver = "abfss://silver@dlsnetflixprojectstorage.dfs.core.windows.net"
gold = "abfss://gold@dlsnetflixprojectstorage.dfs.core.windows.net"

# COMMAND ----------

# Expectation Rules
expectations = {
    "expectation1" : "show_id IS NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "gold_netflixDirectors"
)
@dlt.expect_all_or_drop(expectations)
def createGoldNetflixDirectors():
    df = spark.readStream.format("delta").load(silver + "/netflix_directors")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixCast"
)
@dlt.expect_all_or_drop(expectations)
def createGoldNetflixDirectors():
    df = spark.readStream.format("delta").load(silver + "/netflix_cast")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixCategory"
)
@dlt.expect_all_or_drop(expectations)
def createGoldNetflixDirectors():
    df = spark.readStream.format("delta").load(silver + "/netflix_category")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixCountries"
)
@dlt.expect_or_drop("expectation1" , "show_id IS NOT NULL")
def createGoldNetflixDirectors():
    df = spark.readStream.format("delta").load(silver + "/netflix_countries")
    return df

# COMMAND ----------

@dlt.table

def gold_stg_netflixTitles():
    df = spark.readStream.format("delta").load(silver + "/netflix_titles")
    return df

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

@dlt.view

def gold_transform_netflixTitles():
    df = spark.readStream.table("LIVE.gold_stg_netflixTitles")
    df = df.withColumn("newFlagColumn",lit(1))
    return df

# COMMAND ----------

masterDataExpectations = {
    "expectation1" : "show_id IS NOT NULL",
    "expectation2" : "newFlagColumn IS NOT NULL",
}

# COMMAND ----------

@dlt.table
@dlt.expect_all_or_drop(masterDataExpectations)
def gold_netflixTitles():
    df = spark.readStream.table("LIVE.gold_transform_netflixTitles")
    return df