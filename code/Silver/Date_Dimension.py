# Databricks notebook source
# MAGIC %md
# MAGIC #Loading a specific Data ranges in Date Dimension

# COMMAND ----------

from datetime import *    
from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import datetime,timezone,timedelta

# COMMAND ----------

dt=datetime.today()
date_range=[]
for i in range(-630,1095,1):
    a=dt + timedelta(days=i)
    month = a.month
    day = a.day
    # print(a.strftime("%d %m %Y"))
    if month<10:
        month='0'+str(month)
    if day<10:
        day='0'+str(day)

    date_range.append([str(a.year),str(month),str(day)])

# COMMAND ----------

rdd1=sc.parallelize(date_range)
# rdd1.collect()
schema = "year String,Month String,day String"
df1 = spark.createDataFrame(data=date_range, schema =schema )

# COMMAND ----------

df1=df1.withColumn("daymonconcat",F.expr("concat(concat(year,'-'),month)"))
df1=df1.withColumn("full_Date",F.expr("concat(daymonconcat,'-')"))
df1=df1.withColumn("full_Date",F.expr("concat(full_Date,day)"))
df1=df1.withColumn("date",F.expr("to_date(full_Date)"))
df1=df1.drop("full_Date")
df1=df1.drop("daymonconcat")
df1=df1.withColumn("day_of_the_year",F.expr("dayofyear(date)"))
df1=df1.withColumn("week_of_the_year",F.expr("weekofyear(date)"))


# COMMAND ----------

df1.display()

# COMMAND ----------

df1.repartition(1).write.save('/mnt/Deltalake/silver_Zone/Date_Dimension')
