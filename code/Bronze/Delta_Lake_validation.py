# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

df1=spark.read.option("format",'delta').load("/mnt/Deltalake/Bronze/trip_trans/")

# COMMAND ----------

df1.count()

# COMMAND ----------

df1.write.saveAsTable("trip_transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trip_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC update trip_transactions set driver_id = 'A4' , driver_name ='Aaditya', trip_id=2004 where trip_id is null;

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, '/mnt/Deltalake/Bronze/trip_trans/')

# COMMAND ----------

help(deltaTable)

# COMMAND ----------

deltaTable.update(condition = "driver_id='A1'",set = { "driver_name": "'Ram'" } )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trip_Transactions where trip_id=2003;

# COMMAND ----------

df1.filter(df1.driver_name=="Rama").count()

# COMMAND ----------

df1.filter(df1.driver_name=="Ram").count()

# COMMAND ----------

updatesDF=spark.read.option("Header",True).option("inferschema",True).csv("/mnt/Deltalake/Bronze/export_1.csv")

# COMMAND ----------

deltaTable.alias("trip_transactions_base").merge(
              source = updatesDF.alias("updates"),
              condition = "trip_transactions_base.trip_id = updates.trip_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

df1.filter(df1.trip_id==2004).show(truncate=False)

# COMMAND ----------

files = dbutils.fs.ls("/mnt/Deltalake/Bronze/")
for file in files:
  print(file.path)
