from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view()
def sales_source_mv():
    df= spark.table("ldp_catalog.source.sales")
    df2= df.withColumn('revenue', col("revenue").cast("double"))
    return df2

@dp.materialized_view()
def sales_enriched_mv():
    df= spark.table("sales_source_mv")
    df2= df.withColumn("revenue", col("revenue")*1.25)
    return df2

@dp.materialized_view()
def sales_curated_mv():
    df= spark.table("sales_enriched_mv")
    df2= df.groupBy("store_id").agg(sum('revenue').alias("revenue_per_store"))
    return df2