# Databricks notebook source
# MAGIC %sql
# MAGIC describe ldp_catalog.source.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ldp_catalog.target.sales_curated_stream

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ldp_catalog.source.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ldp_catalog.source.sales
# MAGIC values
# MAGIC (111,1003,3,876,'2022-01-11'),
# MAGIC (112,1104,4,973,'2022-01-12');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparation for Append Flow

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ldp_catalog.source.sigmoid_sales
# MAGIC as select * from ldp_catalog.source.sales where 1=2;
# MAGIC
# MAGIC create table ldp_catalog.source.fractal_sales
# MAGIC as select * from ldp_catalog.source.sales where 1=2;

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table ldp_catalog.source.sigmoid_sales add column brand string;
# MAGIC
# MAGIC alter table ldp_catalog.source.fractal_sales add column brand string;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ldp_catalog.source.fractal_sales
# MAGIC values
# MAGIC (301,3401,21,16000,'2022-01-01','fractal'),
# MAGIC (302,3402,22,17000,'2022-01-02','fractal');

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ldp_catalog.source.sigmoid_sales
# MAGIC values
# MAGIC (601,6401,61,26000,'2022-01-01','sigmoid'),
# MAGIC (602,6402,72,87000,'2022-01-02','sigmoid');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ldp_catalog.source.sigmoid_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparation for Auto CDC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ldp_catalog.source.employee
# MAGIC (
# MAGIC   emp_id int,
# MAGIC   emp_name string,
# MAGIC   emp_location string,
# MAGIC   salary double,
# MAGIC   record_created_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- initial load
# MAGIC INSERT INTO ldp_catalog.source.employee
# MAGIC VALUES
# MAGIC (101,'sudarshan','pune',750.56, current_timestamp()),
# MAGIC (102,'john','delhi',350.56, current_timestamp()),
# MAGIC (103,'sara','mumbai',450.56, current_timestamp()),
# MAGIC (104,'jack','kolkata',550.56, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ldp_catalog.source.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2nd load to validate scd-2
# MAGIC INSERT INTO ldp_catalog.source.employee
# MAGIC VALUES
# MAGIC (101,'sudarshan','Amsterdam',750.56, current_timestamp()),
# MAGIC (102,'john','Seul',350.56, current_timestamp()),
# MAGIC (103,'sara','mumbai',643.75, current_timestamp()),
# MAGIC (104,'jack','kolkata',550.56, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC        (__END_AT IS NULL) AS active
# MAGIC FROM ldp_catalog.target.employee_scd2
