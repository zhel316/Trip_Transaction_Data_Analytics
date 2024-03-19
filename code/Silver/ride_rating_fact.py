# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# COMMAND ----------

df1=spark.read.format("delta").load("/mnt/Deltalake/Bronze/reward_point")

# COMMAND ----------

df1.repartition(1).write.save("/mnt/Deltalake/silver_Zone/ride_Rewards_Points_Fact")
