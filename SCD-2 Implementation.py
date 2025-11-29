# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW customer_src_batch1 AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC   (101, 'Amit Sharma',   'amit@example.com',   'Mumbai',  'MH', '400001', '9876543210'),
# MAGIC   (102, 'Neha Verma',    'neha@example.com',   'Pune',    'MH', '411001', '9876500001'),
# MAGIC   (103, 'Rahul Singh',   'rahul@example.com',  'Delhi',   'DL', '110001', '9876500002'),
# MAGIC   (104, 'Priya Iyer',    'priya@example.com',  'Bangalore','KA','560001','9876500003')
# MAGIC AS t(customer_id, customer_name, email, city, state, postal_code, phone);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW customer_src_batch2 AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   VALUES
# MAGIC     (101, 'Amit Sharma', 'amit@example.com', 'Mumbai', 'MH', '400001', '9876543210'), -- unchanged
# MAGIC     (102, 'Neha Verma', 'neha@example.com', 'Hyderabad', 'TS', '500001', '9876500001'), -- city/state changed
# MAGIC     (103, 'Rahul Singh', 'rahul@example.com', 'Delhi', 'DL', '110001', '9876500002'), -- unchanged
# MAGIC     (104, 'Priya Iyer', 'priya@example.com', 'Bangalore', 'KA', '560001', '9999900000'), -- phone changed
# MAGIC     (105, 'Karan Patel', 'karan@example.com', 'Surat', 'GJ', '395003', '9876500004') AS t -- new customer
# MAGIC     (customer_id, customer_name, email, city, state, postal_code, phone);

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG advanced_analytics_dev;
# MAGIC USE SCHEMA finance_bronze;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dim_customer_scd2 (
# MAGIC   customer_sk   BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   customer_id   INT,
# MAGIC   customer_name STRING,
# MAGIC   email         STRING,
# MAGIC   city          STRING,
# MAGIC   state         STRING,
# MAGIC   postal_code   STRING,
# MAGIC   phone         STRING,
# MAGIC   effective_from DATE,
# MAGIC   effective_to   DATE,
# MAGIC   is_current     BOOLEAN,
# MAGIC   record_hash    STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 3.1 Create a staging view for batch-1 with hash & effective_from
# MAGIC CREATE OR REPLACE TEMP VIEW customer_stage_batch1 AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   city,
# MAGIC   state,
# MAGIC   postal_code,
# MAGIC   phone,
# MAGIC   current_date() AS effective_from,
# MAGIC   CAST(NULL AS DATE) AS effective_to,
# MAGIC   TRUE AS is_current,
# MAGIC   md5(concat_ws('||', customer_name, email, city, state, postal_code, phone)) AS record_hash
# MAGIC FROM
# MAGIC   customer_src_batch1;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG advanced_analytics_dev;
# MAGIC USE SCHEMA finance_bronze;
# MAGIC
# MAGIC -- 3.2 Initial insert (no MERGE needed on day 0)
# MAGIC INSERT INTO dim_customer_scd2
# MAGIC (customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   city,
# MAGIC   state,
# MAGIC   postal_code,
# MAGIC   phone,
# MAGIC   effective_from,
# MAGIC   effective_to,
# MAGIC   is_current,
# MAGIC   record_hash)
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   city,
# MAGIC   state,
# MAGIC   postal_code,
# MAGIC   phone,
# MAGIC   effective_from,
# MAGIC   effective_to,
# MAGIC   is_current,
# MAGIC   record_hash
# MAGIC FROM customer_stage_batch1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from advanced_analytics_dev.finance_bronze.dim_customer_scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- For a real pipeline, this would come from a Bronze/Silver table or Autoloader.
# MAGIC CREATE OR REPLACE TEMP VIEW customer_stage_current_batch AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   city,
# MAGIC   state,
# MAGIC   postal_code,
# MAGIC   phone,
# MAGIC   current_date() AS effective_from,
# MAGIC   md5(concat_ws('||',
# MAGIC       customer_name, email, city, state, postal_code, phone)) AS record_hash
# MAGIC FROM customer_src_batch2;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from advanced_analytics_dev.finance_bronze.dim_customer_scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG advanced_analytics_dev;
# MAGIC USE SCHEMA finance_bronze;
# MAGIC
# MAGIC MERGE INTO dim_customer_scd2 tgt
# MAGIC USING customer_stage_current_batch src
# MAGIC ON src.customer_id = tgt.customer_id AND tgt.is_current= TRUE
# MAGIC WHEN MATCHED AND tgt.record_hash <> src.record_hash THEN UPDATE
# MAGIC SET tgt.effective_to = current_date(),
# MAGIC     tgt.is_current = FALSE;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_sk, city, phone,
# MAGIC        effective_from, effective_to, is_current
# MAGIC FROM dim_customer_scd2
# MAGIC ORDER BY customer_id, effective_from;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG advanced_analytics_dev;
# MAGIC USE SCHEMA finance_bronze;
# MAGIC
# MAGIC INSERT INTO dim_customer_scd2 (
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   city,
# MAGIC   state,
# MAGIC   postal_code,
# MAGIC   phone,
# MAGIC   effective_from,
# MAGIC   effective_to,
# MAGIC   is_current,
# MAGIC   record_hash
# MAGIC )
# MAGIC SELECT
# MAGIC   src.customer_id,
# MAGIC   src.customer_name,
# MAGIC   src.email,
# MAGIC   src.city,
# MAGIC   src.state,
# MAGIC   src.postal_code,
# MAGIC   src.phone,
# MAGIC   src.effective_from,
# MAGIC   CAST(NULL AS DATE) AS effective_to,
# MAGIC   TRUE               AS is_current,
# MAGIC   src.record_hash
# MAGIC FROM customer_stage_current_batch AS src
# MAGIC LEFT JOIN dim_customer_scd2 AS tgt
# MAGIC   ON  tgt.customer_id  = src.customer_id
# MAGIC   AND tgt.is_current   = TRUE
# MAGIC   AND tgt.record_hash  = src.record_hash
# MAGIC WHERE tgt.customer_id IS NULL;      -- no current identical record exists
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_sk, city, phone,
# MAGIC        effective_from, effective_to, is_current
# MAGIC FROM dim_customer_scd2
# MAGIC ORDER BY customer_id, effective_from;
# MAGIC
