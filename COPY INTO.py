# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME advanced_analytics_dev.finance_landed.cft_pool
# MAGIC COMMENT "THIS IS MY MANAGED VOLUME FOR AUTOLOADER DEMO"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's create a placeholder table to copy data from files to a table.
# MAGIC - Placeholder tables in Databricks are special, empty Delta tables that you create before using COPY INTO to load data.
# MAGIC - They act as a target schema for COPY INTO, especially when Databricks cannot infer the schema from the source files.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE advanced_analytics_dev.finance_landed.transactions_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO advanced_analytics_dev.finance_landed.transactions_raw
# MAGIC FROM '/Volumes/advanced_analytics_dev/finance_landed/cft_pool'
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = '*.csv'
# MAGIC FORMAT_OPTIONS (
# MAGIC   'header' = 'true',
# MAGIC   'mergeSchema' = 'true'
# MAGIC )
# MAGIC COPY_OPTIONS (
# MAGIC   'mergeSchema' = 'true'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Even if we run COPY INTO command multiple times on same set of input files, data won't be loaded again which guarantees idempotency.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM advanced_analytics_dev.finance_landed.transactions_raw where category is null;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo: Only loading selective columns from source files into target table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE advanced_analytics_dev.finance_landed.transactions_raw_slct
# MAGIC (
# MAGIC   id string,
# MAGIC   name string,
# MAGIC   amount double,
# MAGIC   load_time timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO advanced_analytics_dev.finance_landed.transactions_raw_slct
# MAGIC FROM (select id, name, cast(amount as double) amount, current_timestamp() as load_time from '/Volumes/advanced_analytics_dev/finance_landed/cft_pool')
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = '*.csv'
# MAGIC FORMAT_OPTIONS (
# MAGIC   'header' = 'true',
# MAGIC   'mergeSchema' = 'true'
# MAGIC )
# MAGIC -- COPY_OPTIONS (
# MAGIC --   'mergeSchema' = 'true'
# MAGIC -- )
# MAGIC
# MAGIC -- We don't need to mention target table's 'mergeSchema' = 'true' parameter since columns are already selected at top.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from advanced_analytics_dev.finance_landed.transactions_raw_slct
