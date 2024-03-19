# Databricks notebook source
# MAGIC %md
# MAGIC # Create Trip Transaction Fact

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# COMMAND ----------

df1=spark.read.format("delta").load("/mnt/Deltalake/Bronze/trip_trans")

# COMMAND ----------

df2=df1.select("trip_id","trip_start_timestamp","trip_end_timestamp","trip_status","total_fare","total_distance","delay_start_time_mins","customer_id",'driver_id')

# COMMAND ----------

df2=df2.withColumn("Trip_Date",F.expr("to_date(trip_start_timestamp)"))

# COMMAND ----------

df2.repartition(1).write.save("/mnt/Deltalake/silver_Zone/Trip_Transactions_Fact")

# COMMAND ----------

# MAGIC %md
# MAGIC #Incremental loads

# COMMAND ----------

df_upsert=spark.read.load("/mnt/Deltalake/Bronze/trip_trans")
df_upsert=df_upsert.withColumn("trip_Date",F.expr("to_date(trip_start_timestamp)"))
df_upsert=df_upsert.filter(df_upsert.trip_Date>=F.expr("Current_date-2"))
df_upsert=df_upsert.select("trip_id","trip_start_timestamp","trip_end_timestamp","trip_status","total_fare","total_distance","delay_start_time_mins","customer_id","driver_id","trip_date")

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, '/mnt/Deltalake/silver_Zone/Trip_Transactions_Fact')

# COMMAND ----------

deltaTable.alias("trip_transactions_base").merge(
              source = df_upsert.alias("updates"),
              condition = "trip_transactions_base.trip_id = updates.trip_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

spark.read.load('/mnt/Deltalake/silver_Zone/Trip_Transactions_Fact').select("Trip_Date")
