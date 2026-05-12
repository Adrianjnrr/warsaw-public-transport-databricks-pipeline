from pyspark.sql.functions import col 
from utils.metadata import add_silver_metadata

def build_stop_silver(spark):
    stops = spark.table('bronze.stops')

    stop_silver = (
        stops
        .select(
            col('stop_id').cast('string'),
            col('stop_name').cast('string')
        )
        .filter(col('stop_id').isNotNull())
        .dropDuplicates(['stop_id'])
    )  
    stop_silver = add_silver_metadata(stop_silver)

    print(f"Silver Stop count: {stop_silver.count()}")

    stop_silver.write.mode('overwrite').option('overwriteSchema', 'true').format('delta').saveAsTable('silver.stops')

    return stop_silver