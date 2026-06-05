# Databricks notebook source
df=spark.read.csv("/Volumes/main/default/Volume/SalesPipeline/Sales_Details.csv",inferSchema=True,header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

df.write.format("delta").saveAsTable("Bronze_Sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_sales