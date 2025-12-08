# BPM_OP_Automation.py - Logic and Implementation Documentation

## Overview

The `BPM_OP_Automation.py` notebook is a Databricks pipeline that generates and stores forecasted financial metrics for future financial periods. It focuses on processing key financial indicators including:

- **GSV** (Gross Sales Value)
- **OnInvoiceTI** (On-Invoice Trade Investment)
- **NIV** (Net Invoice Value = GSV + OnInvoiceTI)
- **OffInvoiceTI** (Off-Invoice Trade Investment)
- **AdvertisingAndPromotions**
- **Overhead**
- **OperatingProfit**

The pipeline leverages Spark SQL, Delta Lake, and implements sophisticated incremental load logic with currency conversion capabilities.

---

## Table of Contents

1. [Setup and Configuration](#setup-and-configuration)
2. [Data Source and Aggregation](#data-source-and-aggregation)
3. [Currency Conversion](#currency-conversion)
4. [Incremental Load Logic](#incremental-load-logic)
5. [Functions from Routines.py](#functions-from-routinespy)
6. [Data Retention](#data-retention)
7. [Final Steps](#final-steps)

---

## Setup and Configuration

### Date Calculations

The notebook starts by calculating key date variables for the pipeline execution:

```python
# Current date in YYYYMM format
current_date_yyyymm = current_date.strftime("%Y%m")

# Add 5 months to current date (6 months ahead)
new_date = current_date + relativedelta(months=5)

# Format as FYY_PMM (e.g., F25_P10)
new_date_fyy_pmm = f"F{new_date.strftime('%y')}_P{new_date.strftime('%m')}"

# Run date in YYYYMM format (e.g., 202510)
run_date = new_date.strftime("%Y%m")

# Previous month run date (for incremental comparison)
past_month_run_date = (dt - relativedelta(months=1)).strftime("%Y%m")
```

**Key Variables:**
- `run_date`: The fiscal period being processed (e.g., 202510 for October 2025)
- `past_month_run_date`: Previous month's run date for comparison
- `new_date_fyy_pmm`: Financial period format (e.g., F25_P10)

### Target Table Creation

The notebook creates a Delta table `ddh_bpm_op` if it doesn't exist, with the following schema:

- **Dimension Columns**: CurrencyISOCode, PartitionMonth, FiscalPeriodId, FinancialYearName, LedgerTypeName, ProfitCentreCode, ProfitCentreName, CustomerCode, CustomerName, PlanningAccount, OrgChannel, OrgSubChannel
- **Financial Metrics** (in local currency): GSV, OnInvoiceTI, NIV, OffInvoiceTI, AdvertisingAndPromotions, Overhead, OperatingProfit
- **Metadata**: RunPeriodFiscal, CurrencyBudgetRate
- **USD Converted Metrics**: All financial metrics with `_USD` suffix

---

## Data Source and Aggregation

### SQL Query Logic

The main data aggregation query (`bpm_op_query`) performs the following operations:

1. **Source Tables**: Joins multiple dimension and fact tables from the DDH production schema:
   - `tb_factshipmentledger` (fact table)
   - `tb_dimledgertype` (ledger type dimensions)
   - `tb_dimprofitcentre` (profit center dimensions)
   - `tb_dimproductharmonized` (product dimensions)
   - `tb_dimcustomerharmonized` (customer dimensions)
   - `tb_dimgeography` (geography dimensions)
   - `tb_dimdate` (date dimensions)

2. **Filtering Logic**:
   - Excludes unknown currency codes (`CurrencyISOCode NOT IN ('Unknown','N/A')`)
   - Handles Actual vs Forecast data:
     - **Forecast**: `PeriodClosed = false AND LedgerTypeName = 'Forecast'`
     - **Actual**: `PeriodClosed = true AND LedgerTypeName = 'Actual'`
   - Filters fiscal periods from 202501 onwards
   - Excludes adjustment profit centers (`ProfitCentreCode NOT LIKE '%ADJ%'`)

3. **Aggregation**: Uses conditional SUM aggregation to calculate financial metrics based on `pnlcodedescription`:
   - `GrossSalesValue` → GSV
   - `OnInvoiceTradeInvestment` → OnInvoiceTI
   - `GrossSalesValue + OnInvoiceTradeInvestment` → NIV
   - `OffInvoiceTradeInvestment` → OffInvoiceTI
   - `AdvertisingAndPromotions` → AdvertisingAndPromotions
   - `Overhead` → Overhead
   - `OperatingProfit` → OperatingProfit

4. **Grouping**: Groups by all dimension columns to create granular records

### Additional Processing

- Adds `RunPeriodFiscal` column with the current run date
- Filters data based on `bpm_fiscal_period_filter` parameter
- Drops `TheDate` column as it's not needed in the final output

---

## Currency Conversion

### Budget Rate Join

The notebook joins the source data with budget exchange rates from `ddh_budgetrate` table:

```python
df_budget_rate = df_budget_rate.select(
    col("TargetCurrencyKey").alias("CurrencyISOCode"), 
    "CurrencyBudgetRate"
)

df_bpm_with_rate = df_bpm_filter.join(
    df_budget_rate, 
    on="CurrencyISOCode", 
    how="left"
)
```

### USD Conversion

For each financial metric, the notebook calculates USD equivalents:

- **USD Currency**: If `CurrencyISOCode == "USD"`, the rate is set to 1.0
- **Other Currencies**: Values are divided by `CurrencyBudgetRate` to get USD equivalents

**Converted Metrics:**
- GSV_USD
- OnInvoiceTI_USD
- NIV_USD
- OffInvoiceTI_USD
- AdvertisingAndPromotions_USD
- Overhead_USD
- OperatingProfit_USD

---

## Incremental Load Logic

The notebook implements sophisticated incremental load logic to handle updates and prevent duplicates.

### Initial Load Check

```python
if data_in_target_table_exists == False:
    append_data_def(df_bpm_with_rate_usd, UNITY_CATALOG, SCHEMA_BRONZE, "ddh_bpm_op")
    dbutils.notebook.exit("Target table loaded for the first time...")
```

If the table is empty, it performs an initial load and exits.

### Incremental Processing

For subsequent runs, the notebook:

1. **Separates Data**:
   - `incoming_df`: Current run period data (`RunPeriodFiscal = run_date`)
   - `existing_df`: Previous run period data (`RunPeriodFiscal = past_month_run_date`)

2. **Identifies Join Columns**: Uses key dimensions for joining:
   - CurrencyISOCode, PartitionMonth, FiscalPeriodId, FinancialYearName
   - ProfitCentreCode, ProfitCentreName
   - CustomerCode, CustomerName
   - PlanningAccount, OrgChannel, OrgSubChannel
   - CurrencyBudgetRate

3. **Renames Columns**: To distinguish between incoming and existing data:
   - Incoming columns: `{metric}_incoming`
   - Existing columns: `{metric}_existing`

4. **Outer Join**: Performs outer join to capture all records from both datasets

---

## Functions from Routines.py

The notebook extensively uses functions from `Routines.py`. Here are the key functions:

### 1. `append_data_def()`

**Location**: Lines 757-765 in Routines.py

**Purpose**: Appends a DataFrame to a Delta table with schema merging enabled.

**Usage in BPM_OP_Automation**:
```python
append_data_def(df_bpm_with_rate_usd, UNITY_CATALOG, SCHEMA_BRONZE, "ddh_bpm_op")
```

**Function Logic**:
- Takes DataFrame, Unity Catalog, schema, and table name
- Writes in append mode with `mergeSchema = true`
- Prints success message with record count

---

### 2. `update_df_through_ledgertype_bpm_op()`

**Location**: Lines 725-750 in Routines.py

**Purpose**: Updates financial metric columns based on ledger type priority logic (Actual vs Forecast).

**Usage in BPM_OP_Automation**: Called 14 times (once for each metric in local currency and USD)

**Function Signature**:
```python
def update_df_through_ledgertype_bpm_op(
    df: SparkDataFrame, 
    col_name: str, 
    existing_col: str, 
    incoming_col: str
) -> SparkDataFrame
```

**Priority Logic** (in order):

1. **Both Actual**: If both incoming and existing are "actual", keep existing value
   ```python
   when(
       lower(col("LedgerTypeName_incoming")).contains("actual")
       & lower(col("LedgerTypeName_existing")).contains("actual"),
       col(existing_col)
   )
   ```

2. **Incoming Actual, Existing Forecast**: Prefer incoming actual (more recent actual data)
   ```python
   when(
       lower(col("LedgerTypeName_incoming")).contains("actual")
       & lower(col("LedgerTypeName_existing")).contains("forecast"),
       col(incoming_col)
   )
   ```

3. **Both Forecast**: Prefer incoming forecast (more recent forecast)
   ```python
   when(
       lower(col("LedgerTypeName_incoming")).contains("forecast")
       & lower(col("LedgerTypeName_existing")).contains("forecast"),
       col(incoming_col)
   )
   ```

4. **Null Handling**: If incoming is null but existing has value, keep existing
   ```python
   when(
       col(incoming_col).isNull() & col(existing_col).isNotNull(),
       col(existing_col)
   )
   ```

5. **Default**: Otherwise, use incoming value

**Metrics Updated** (in sequence):
1. GSV
2. OnInvoiceTI
3. NIV
4. OffInvoiceTI
5. AdvertisingAndPromotions
6. Overhead
7. OperatingProfit
8. GSV_USD
9. OnInvoiceTI_USD
10. NIV_USD
11. OffInvoiceTI_USD
12. AdvertisingAndPromotions_USD
13. Overhead_USD
14. OperatingProfit_USD

Each metric update is chained, with the output of one becoming the input to the next.

---

### 3. `cleanup_old_data()`

**Location**: Lines 864-945 in Routines.py

**Purpose**: Deletes data older than a specified retention period (default: 13 months) based on `RunPeriodFiscal`.

**Usage in BPM_OP_Automation**:
```python
cleanup_old_data(UNITY_CATALOG, SCHEMA_BRONZE, "ddh_bpm_op")
```

**Function Logic**:

1. **Get Maximum Period**: Queries the table for the maximum `RunPeriodFiscal`
2. **Calculate Cutoff**: Subtracts retention months (13) from max period
3. **Delete Old Data**: Removes all records where `RunPeriodFiscal <= cutoff_period`
4. **Error Handling**: Gracefully handles cases where table doesn't exist

**Example**:
- If max `RunPeriodFiscal` is `202510` (October 2025)
- Cutoff = 202510 - 13 months = 202409 (September 2024)
- All data with `RunPeriodFiscal <= 202409` is deleted

---

## Data Merging and Finalization

### LedgerTypeName Handling

After updating all metrics, the notebook handles `LedgerTypeName`:

```python
ledgertypename_df = operatingprofit_usd_df.withColumn(
    "LedgerTypeName",
    when(
        col("LedgerTypeName_incoming").isNull()
        & col("LedgerTypeName_existing").isNotNull(),
        col("LedgerTypeName_existing")
    ).otherwise(col("LedgerTypeName_incoming"))
)
```

**Logic**: Prefer incoming value, but if incoming is null and existing has value, use existing.

### RunPeriodFiscal Handling

Updates `RunPeriodFiscal` to ensure consistency:

```python
runperiodfiscal_df = ledgertypename_df.withColumn(
    "RunPeriodFiscal",
    when(
        col("RunPeriodFiscal_incoming").isNull()
        & col("RunPeriodFiscal_existing").isNotNull(),
        lit(int(run_date))
    ).otherwise(col("RunPeriodFiscal_incoming"))
)
```

**Logic**: Use incoming value, but if missing, set to current run date.

### Column Cleanup

Removes all temporary columns with `_existing` or `_incoming` suffixes:

```python
cols_to_drop = [
    c for c in runperiodfiscal_df.columns
    if c.endswith("_existing") or c.endswith("_incoming")
]
final_df = runperiodfiscal_df.drop(*cols_to_drop)
```

### Schema Validation

Validates that the final DataFrame has the same number of columns as both incoming and existing DataFrames:

```python
assert len(existing_df.columns) == len(final_df.columns)
assert len(incoming_df.columns) == len(final_df.columns)
```

---

## Final Data Load

The notebook implements three scenarios for loading data:

### Scenario 1: New Period (Append)

```python
if existing_max_run_period < int(run_date):
    final_df.write.mode("append")
        .option("userMetadata", refresh_run_id)
        .option("mergeSchema", "true")
        .format("delta")
        .saveAsTable(table_name)
```

**Condition**: Current run period is newer than existing max period  
**Action**: Append new data

### Scenario 2: Same Period (Replace)

```python
elif existing_max_run_period == int(run_date):
    delete_query = f"""DELETE FROM {table_name} WHERE RunPeriodFiscal = {existing_max_run_period}"""
    spark.sql(delete_query)
    
    final_df.write.mode("append")
        .option("mergeSchema", "true")
        .option("userMetadata", refresh_run_id)
        .saveAsTable(table_name)
```

**Condition**: Current run period matches existing max period (handles multiple refreshes in same month)  
**Action**: Delete existing data for that period, then append new data

### Scenario 3: Old Period (Skip)

```python
else:
    print(f"Error! Trying to insert old data. Skipping load...")
```

**Condition**: Current run period is older than existing max period  
**Action**: Skip load to prevent data corruption

---

## Data Retention

After loading data, the notebook calls `cleanup_old_data()` to maintain data retention policy:

- **Retention Period**: 13 months
- **Purpose**: Keeps table size manageable and removes outdated data
- **Implementation**: Uses `RunPeriodFiscal` to identify old records

---

## Final Steps

### Table Tagging

The notebook adds a table property to identify it as part of the Cash Forecasting Tool:

```python
spark.sql(
    f"""ALTER TABLE {UNITY_CATALOG}.{SCHEMA_BRONZE}.ddh_bpm_op 
    SET TBLPROPERTIES ('project_name' = 'CashForecastingTool');"""
)
```

---

## Key Design Patterns

### 1. Incremental Load Strategy

The notebook uses a sophisticated incremental load approach:
- Compares incoming data with previous period's data
- Merges based on ledger type priority (Actual > Forecast)
- Handles multiple refreshes in the same period

### 2. Currency Handling

- Supports multi-currency data
- Converts all metrics to USD for standardization
- Handles USD currency specially (rate = 1.0)

### 3. Data Quality

- Validates schema consistency
- Handles null values appropriately
- Excludes invalid data (unknown currencies, adjustment PCs)

### 4. Performance Optimization

- Uses Delta Lake for ACID transactions
- Implements data retention to manage table size
- Leverages Spark SQL for efficient aggregations

---

## Summary

The `BPM_OP_Automation.py` notebook is a comprehensive ETL pipeline that:

1. **Aggregates** financial metrics from multiple source tables
2. **Converts** currencies to USD for standardization
3. **Merges** incremental data using sophisticated ledger type priority logic
4. **Manages** data retention and table lifecycle
5. **Ensures** data quality through validation and error handling

The implementation heavily relies on reusable functions from `Routines.py`, particularly:
- `append_data_def()` for initial loads
- `update_df_through_ledgertype_bpm_op()` for incremental merging logic
- `cleanup_old_data()` for data retention management

This design promotes code reusability, maintainability, and consistency across the Cash Forecasting Tool project.

