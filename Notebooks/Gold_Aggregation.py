# Databricks notebook source
silver_df=spark.table("silver_sales")

# COMMAND ----------

from pyspark.sql.functions import *
gold_df=silver_df.groupBy("region").agg(sum("Revenue").alias("Regional_Revenue"))

# COMMAND ----------

gold_df.show()

# COMMAND ----------

gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_sales")


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table gold_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

gold_region_sales=silver_df.groupBy("region").agg(sum("revenue").alias("regional_revenue"))


# COMMAND ----------

gold_region_sales.write.format("delta").mode("overwrite").saveAsTable("Gold_Regional_Sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

gold_products_sales=silver_df.groupby("product").agg(sum("Revenue").alias("product_revenue"))

# COMMAND ----------

gold_products_sales.write.format("delta").mode("overwrite").saveAsTable("gold_product_sales")

# COMMAND ----------

gold_customer_sales=silver_df.groupby("customer_id").agg(sum("Revenue").alias("customer_revenue"))
gold_customer_sales.write.format("delta").mode("overwrite").saveAsTable("gold_customer_sales")

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_customer_sales
# MAGIC order by customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history silver_sales

# COMMAND ----------

silver_df.write.format("delta").partitionBy("region").mode("overwrite").saveAsTable("Silver_partitioned_Region")


# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail silver_partitioned_region