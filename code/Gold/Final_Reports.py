# Databricks notebook source
# MAGIC %md
# MAGIC #Creating a Delta Table in Gold Zone with below Details:
# MAGIC ##1.Fetching the highest number of rides by month per driver and  highest number of trips and highest spent customer by month & by the year.
# MAGIC ##2.Fetching the top rated driver for by the year. 
# MAGIC ##3.Fetching the highest spent customer & highest distance travelled customer.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import broadcast
import pyspark.sql.functions as F

# COMMAND ----------

df1=spark.read.load("/mnt/Deltalake/silver_Zone/Trip_Transactions_Fact")

# COMMAND ----------

df2=spark.read.load("/mnt/Deltalake/silver_Zone/Customer_Dimension")

# COMMAND ----------

df3=spark.read.load("/mnt/Deltalake/silver_Zone/driver_Dimension")

# COMMAND ----------

df4=spark.read.load("/mnt/Deltalake/silver_Zone/Date_Dimension")

# COMMAND ----------

df5=df1.join(broadcast(df2),df1.customer_id==df2.customer_id)

# COMMAND ----------

df5.rdd.getNumPartitions()

# COMMAND ----------

df5=df5.select("trip_id","Customer_Name","customer_age","customer_gender","Trip_Date","driver_id","total_distance","total_fare","driver_id")

# COMMAND ----------

df6=df5.join(broadcast(df3),df3.driver_id==df5.driver_id)

# COMMAND ----------

df6=df6.select("trip_id","Customer_Name","customer_age","customer_gender","Trip_Date","total_distance","total_fare","driver_name","driver_age","driver_gender")

# COMMAND ----------

df7=df6.join(df4,df4.date==df6.Trip_Date)

# COMMAND ----------

df7.createOrReplaceTempView("df7")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Highest Spent & Highest distance travelled by Customer

# COMMAND ----------

df_customer_spent_distance=spark.sql("select customer_name, rank_total_distance,rank_total_fare ,total_distance,total_fare , rank_trips_count ,concat(month,year) as month_year,trips_count from (select customer_name, rank() over(partition by month,year order by total_distance desc)as rank_total_distance, rank() over(partition by month,year order by total_fare  desc) as rank_total_fare, total_distance,total_fare,month,year, rank() over(partition by month,year order by trips_count desc ) as rank_trips_count,trips_count from (select customer_name,sum(total_fare) as total_fare,month,year,sum(total_distance) as total_distance,count(trip_id) as trips_count from df7 group by customer_name,month,year)) where rank_total_distance=1 or rank_total_fare=1 or rank_trips_count=1 order by concat(month,year)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Highest Rating & Highest Trips travelled by Driver

# COMMAND ----------

df_driver_trips=spark.sql("select driver_name, concat(month,year) as year_Month,count_trips from (select driver_name, rank() over(partition by month,year order by count_trips desc )as rank_count_trips,month,year,count_trips from (select driver_name,month,year,count(trip_id) as count_trips from df7 group by driver_name,month,year)) where rank_count_trips=1  order by concat(month,year)")

# COMMAND ----------

df8=spark.read.load("/mnt/Deltalake/Bronze/reward_point")

# COMMAND ----------

df9=df8.join(df7,df7.trip_id==df8.trip_id)

# COMMAND ----------

df10=df9.groupBy("Customer_Name","Month","year").sum("customer_rating")

# COMMAND ----------

df11=df10.withColumnRenamed("sum(customer_rating)","Customer_Rating")

# COMMAND ----------

df12=df9.groupBy("driver_name","Month","year").sum("driver_rating")

# COMMAND ----------

df13=df12.withColumnRenamed("sum(driver_rating)","driver_Rating")

# COMMAND ----------

df14=df13.withColumn("rank_driver_rating",F.expr("rank() over(partition by concat(month,Year) order by driver_rating desc)"))

# COMMAND ----------

df15=df14.filter(df14.rank_driver_rating==1)

# COMMAND ----------

df15.display()

# COMMAND ----------

df12=df11.groupBy("customer_name","month","year").sum("customer_rating")
df12=df12.withColumnRenamed("sum(customer_rating)","customer_rating")
df13=df12.withColumn("rank_customer_rating",F.expr("rank() over(partition by concat(month,Year) order by customer_rating desc)"))

# COMMAND ----------

df15=df13.filter(df13.rank_customer_rating==1)

# COMMAND ----------

df15.display()

# COMMAND ----------

df_customer_spent_distance.write.saveAsTable("df_customer_spent_distance")
