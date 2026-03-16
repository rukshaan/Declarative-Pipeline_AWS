from pyspark import pipelines as dp
from pyspark.sql import functions as fn

@dp.materialized_view(
    name="transportation.silver.silver_city",
    comment="City Raw Data Processing",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_silver() :
    df_bronze = spark.read.table("transportation.bronze.bronze_city")
    df_silver = df_bronze.select(
        fn.col("city_id").alias("city_id"),
        fn.col("city_name").alias("city_name"),
        fn.col("ingest_datetime").alias("bronze_ingest_datetime")
        
    )
    df_silver =df_silver.withColumn("silver_ingest_datetime", fn.current_timestamp())
    
    return df_silver