# Databricks notebook source
dbutils.widgets.text("Weekday Number",'7')

# COMMAND ----------

var_wn = int(dbutils.widgets.get("Weekday Number"))

# COMMAND ----------

dbutils.jobs.taskValues.set(key="weekDayNum", value=var_wn)