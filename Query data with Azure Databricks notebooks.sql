-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Query data with Azure Databricks notebooks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC This tutorial walks you through using the Azure Databricks notebooks user interface to create a cluster and a notebook, create a table from a dataset, query the table, and display the query results.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Write the CSV data to Delta Lake format.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC diamonds = spark.read \
-- MAGIC     .format("csv")\
-- MAGIC     .option("header","true")\
-- MAGIC     .option("inferSchema","true")\
-- MAGIC     .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
-- MAGIC
-- MAGIC diamonds.write.format("delta").mode("overwrite").save("/mnt/delta/diamonds")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Display the data to see what it looks like.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(diamonds)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Create a Delta table at the stored location.

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;
CREATE TABLE diamonds USING DELTA LOCATION "/mnt/delta/diamonds/";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Query the table for the average diamond price by color.

-- COMMAND ----------

SELECT color, avg(price) AS price
FROM diamonds
GROUP BY color
ORDER BY color;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Display a chart of the average diamond price by color.

-- COMMAND ----------

SELECT color, avg(price) AS price
FROM diamonds
GROUP BY color
ORDER BY color;

-- COMMAND ----------


