# Databricks notebook source
bronze_df=spark.table("Bronze_Sales")

# COMMAND ----------

bronze_df.count()

# COMMAND ----------

from pyspark.sql.functions import *
bronze_df.select([count(when(col(c).isNull(),c)).alias(c)
    for c in bronze_df.columns]).show()

# COMMAND ----------

silver_df=bronze_df.na.drop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_sales

# COMMAND ----------

silver_df=silver_df.dropDuplicates()

# COMMAND ----------

silver_df=silver_df.withColumn("Revenue",col("price")*col("quantity"))


# COMMAND ----------

silver_df=silver_df.withColumn("order_date",to_date(col("order_date")))

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables