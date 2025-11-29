# Databricks notebook source
# MAGIC %md
# MAGIC # Optimized SCD-2 Type 2 Implementation
# MAGIC 
# MAGIC This notebook demonstrates an optimized SCD-2 implementation using a single MERGE statement.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Set your catalog and schema here

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Configuration: Update these values as needed
# MAGIC SET var.catalog = 'advanced_analytics_dev';
# MAGIC SET var.schema = 'finance_bronze';
# MAGIC SET var.table_name = 'dim_customer_scd2';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Sample Source Data (Batch 1 - Initial Load)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create initial batch of customer data
# MAGIC CREATE OR REPLACE TEMP VIEW customer_src_batch1 AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC   (101, 'Amit Sharma',   'amit@example.com',   'Mumbai',  'MH', '400001', '9876543210'),
# MAGIC   (102, 'Neha Verma',    'neha@example.com',   'Pune',    'MH', '411001', '9876500001'),
# MAGIC   (103, 'Rahul Singh',   'rahul@example.com',  'Delhi',   'DL', '110001', '9876500002'),
# MAGIC   (104, 'Priya Iyer',    'priya@example.com',  'Bangalore','KA','560001','9876500003')
# MAGIC AS t(customer_id, customer_name, email, city, state, postal_code, phone);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Target Dimension Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use configured catalog and schema
# MAGIC USE CATALOG ${var.catalog};
# MAGIC USE SCHEMA ${var.schema};
# MAGIC
# MAGIC -- Create SCD-2 dimension table
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Initial Load (Batch 1)
# MAGIC For the initial load, we can use a simple INSERT since there are no existing records to merge.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${var.catalog};
# MAGIC USE SCHEMA ${var.schema};
# MAGIC
# MAGIC -- Create staging view for batch 1 with hash calculation
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
# MAGIC USE CATALOG ${var.catalog};
# MAGIC USE SCHEMA ${var.schema};
# MAGIC
# MAGIC -- Initial insert (no MERGE needed for first load)
# MAGIC INSERT INTO dim_customer_scd2
# MAGIC (
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

# MAGIC %md
# MAGIC ## Step 4: Verify Initial Load

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${var.catalog};
# MAGIC USE SCHEMA ${var.schema};
# MAGIC
# MAGIC SELECT 
# MAGIC   customer_sk,
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   city,
# MAGIC   phone,
# MAGIC   effective_from,
# MAGIC   effective_to,
# MAGIC   is_current
# MAGIC FROM dim_customer_scd2
# MAGIC ORDER BY customer_id, effective_from;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Incremental Batch (Batch 2)
# MAGIC This batch contains:
# MAGIC - Unchanged records (101, 103)
# MAGIC - Changed records (102: city/state changed, 104: phone changed)
# MAGIC - New record (105)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create incremental batch with changes
# MAGIC CREATE OR REPLACE TEMP VIEW customer_src_batch2 AS
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC   (101, 'Amit Sharma', 'amit@example.com', 'Mumbai', 'MH', '400001', '9876543210'), -- unchanged
# MAGIC   (102, 'Neha Verma', 'neha@example.com', 'Hyderabad', 'TS', '500001', '9876500001'), -- city/state changed
# MAGIC   (103, 'Rahul Singh', 'rahul@example.com', 'Delhi', 'DL', '110001', '9876500002'), -- unchanged
# MAGIC   (104, 'Priya Iyer', 'priya@example.com', 'Bangalore', 'KA', '560001', '9999900000'), -- phone changed
# MAGIC   (105, 'Karan Patel', 'karan@example.com', 'Surat', 'GJ', '395003', '9876500004') -- new customer
# MAGIC AS t(customer_id, customer_name, email, city, state, postal_code, phone);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Optimized SCD-2 MERGE (Single Operation)
# MAGIC This single MERGE statement handles:
# MAGIC - **WHEN MATCHED**: Closes old records when data has changed (sets effective_to and is_current = FALSE)
# MAGIC - **WHEN NOT MATCHED**: Inserts new records (handles both new customers and changed records that were just closed)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${var.catalog};
# MAGIC USE SCHEMA ${var.schema};
# MAGIC
# MAGIC -- Create staging view for current batch
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
# MAGIC   md5(concat_ws('||', customer_name, email, city, state, postal_code, phone)) AS record_hash
# MAGIC FROM customer_src_batch2;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${var.catalog};
# MAGIC USE SCHEMA ${var.schema};
# MAGIC
# MAGIC -- OPTIMIZED: Single MERGE statement handles both closing old records and inserting new/changed records
# MAGIC MERGE INTO dim_customer_scd2 AS tgt
# MAGIC USING customer_stage_current_batch AS src
# MAGIC ON src.customer_id = tgt.customer_id AND tgt.is_current = TRUE
# MAGIC WHEN MATCHED AND tgt.record_hash <> src.record_hash THEN
# MAGIC   -- Close the old record when data has changed
# MAGIC   UPDATE SET
# MAGIC     tgt.effective_to = current_date(),
# MAGIC     tgt.is_current = FALSE
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   -- Insert new records (handles both new customers and changed records that were just closed)
# MAGIC   INSERT (
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     email,
# MAGIC     city,
# MAGIC     state,
# MAGIC     postal_code,
# MAGIC     phone,
# MAGIC     effective_from,
# MAGIC     effective_to,
# MAGIC     is_current,
# MAGIC     record_hash
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.customer_id,
# MAGIC     src.customer_name,
# MAGIC     src.email,
# MAGIC     src.city,
# MAGIC     src.state,
# MAGIC     src.postal_code,
# MAGIC     src.phone,
# MAGIC     src.effective_from,
# MAGIC     CAST(NULL AS DATE),
# MAGIC     TRUE,
# MAGIC     src.record_hash
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Verify Final Results

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${var.catalog};
# MAGIC USE SCHEMA ${var.schema};
# MAGIC
# MAGIC -- View all records with history
# MAGIC SELECT 
# MAGIC   customer_sk,
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   city,
# MAGIC   state,
# MAGIC   phone,
# MAGIC   effective_from,
# MAGIC   effective_to,
# MAGIC   is_current
# MAGIC FROM dim_customer_scd2
# MAGIC ORDER BY customer_id, effective_from;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Validation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${var.catalog};
# MAGIC USE SCHEMA ${var.schema};
# MAGIC
# MAGIC -- Check current records only
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   city,
# MAGIC   phone,
# MAGIC   effective_from,
# MAGIC   is_current
# MAGIC FROM dim_customer_scd2
# MAGIC WHERE is_current = TRUE
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${var.catalog};
# MAGIC USE SCHEMA ${var.schema};
# MAGIC
# MAGIC -- Verify no duplicate current records per customer
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   COUNT(*) AS current_record_count
# MAGIC FROM dim_customer_scd2
# MAGIC WHERE is_current = TRUE
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC The optimized implementation uses a **single MERGE statement** that:
# MAGIC 1. Closes old records when data changes (WHEN MATCHED)
# MAGIC 2. Inserts new/changed records in one operation (WHEN NOT MATCHED)
# MAGIC 
# MAGIC This eliminates the need for a separate INSERT statement and improves performance.

