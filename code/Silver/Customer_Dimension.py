# Databricks notebook source
# MAGIC %md
# MAGIC # Create Customer Dimension

# COMMAND ----------

import pyspark.sql.functions as F
from random import randint
from pyspark.sql.types import * 

# COMMAND ----------

df1=spark.read.format("delta").load("/mnt/Deltalake/Bronze/trip_trans")

# COMMAND ----------

df1.createOrReplaceTempView("df1") 

# COMMAND ----------

df2=spark.sql('select distinct customer_id,customer_name from df1')

# COMMAND ----------

def customer_age(customer_id):
    return randint(18,65)
def customer_gender(customer_id):
    if customer_id%2==0:
        return 'M'
    else:
        return "F"

# COMMAND ----------

spark.udf.register("customer_age", customer_age,IntegerType())
spark.udf.register("customer_gender", customer_gender,StringType())

# COMMAND ----------

df3=df2.withColumn("customer_age",F.expr("customer_age(customer_id)"))
df3=df3.withColumn("customer_gender",F.expr("customer_gender(customer_id)"))

# COMMAND ----------

display(df3)

# COMMAND ----------

df3.repartition(1).write.save("/mnt/Deltalake/silver_Zone/Customer_Dimension")

# COMMAND ----------

spark.read.load("/mnt/Deltalake/silver_Zone/Customer_Dimension").display()
