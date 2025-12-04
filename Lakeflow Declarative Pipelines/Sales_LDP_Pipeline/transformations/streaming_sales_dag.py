from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table()
def sales_source_stream():
    df= spark.readStream.table("ldp_catalog.source.sales")
    df2= df.withColumn('revenue', col("revenue").cast("double"))
    return df2

@dp.table()
def sales_enriched_stream():
    df= spark.readStream.table("sales_source_stream")
    df2= df.withColumn("revenue", col("revenue")*1.25)
    return df2

@dp.table()
def sales_curated_stream():
    df= spark.readStream.table("sales_enriched_stream")
    df2= df.groupBy("store_id").agg(sum('revenue').alias("revenue_per_store"))
    return df2