from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_streaming_table('analytics_sales')

@dp.append_flow(target='analytics_sales')
def fractal_sales():
    df= spark.readStream.table("ldp_catalog.source.fractal_sales")
    return df

@dp.append_flow(target='analytics_sales')
def sigmoid_sales():
    df= spark.readStream.table("ldp_catalog.source.sigmoid_sales")
    return df
    
