from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_streaming_table('employee_scd2')

@dp.temporary_view()
def employee_source():
    df= spark.readStream.table("ldp_catalog.source.employee")
    return df

dp.create_auto_cdc_flow(
  target = "employee_scd2",
  source = "employee_source",
  keys = ["emp_id"],
  sequence_by = col("record_created_at"),
  except_column_list = ["record_created_at"],
  stored_as_scd_type = "2" # for SCD-1, we need to put "1" in here.
)


    
