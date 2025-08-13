# Databricks notebook source
vary = dbutils.jobs.taskValues.get(taskKey="weekDay_Lookup", key="weekDayNum")

# COMMAND ----------

print(vary)