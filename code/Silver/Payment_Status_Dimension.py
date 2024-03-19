# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# COMMAND ----------

df1=spark.read.format("delta").load("/mnt/Deltalake/Bronze/trip_trans")

# COMMAND ----------

df2=df1.select("trip_id","payment_method","payment_Status","trip_start_timestamp")

# COMMAND ----------

df2=df2.withColumn("Due_Date",F.expr("to_date(trip_start_timestamp)+7"))

# COMMAND ----------

df2.repartition(1).write.save("/mnt/Deltalake/silver_Zone/Payment_Status_Dimension")
