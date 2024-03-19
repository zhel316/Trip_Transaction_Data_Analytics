# Databricks notebook source
# MAGIC %md
# MAGIC # Create Driver Dimension

# COMMAND ----------

import pyspark.sql.functions as F
from random import randint
from pyspark.sql.types import * 

# COMMAND ----------

df1=spark.read.format("delta").load("/mnt/Deltalake/Bronze/trip_trans")

# COMMAND ----------

df1.createOrReplaceTempView("df1")

# COMMAND ----------

df2=spark.sql('select distinct driver_id,driver_name from df1')

# COMMAND ----------

def driver_age(driver_id):
    return randint(18,65)
def driver_gender(driver_id):
    if randint(0,2)==0:
        return 'F'
    else:
        return "M"
    

# COMMAND ----------

spark.udf.register("driver_age", driver_age,IntegerType())
spark.udf.register("driver_gender", driver_gender,StringType())

# COMMAND ----------

df2=df2.withColumn("driver_age",F.expr("driver_age(driver_id)"))
df2=df2.withColumn("driver_gender",F.expr("driver_gender(driver_id)"))

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.repartition(1).write.save("/mnt/Deltalake/silver_Zone/driver_Dimension")
