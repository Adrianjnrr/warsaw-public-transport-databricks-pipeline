from pyspark.sql.functions import col
from utils.metadata import add_gold_metadata

def build_dim_trips(spark):
    gold_trips = spark.table('silver.trips')  
    dim_trips = (
        gold_trips
        .select(
            col('trip_id').cast('string'),
            col('route_id').cast('string')
        )
        .filter(
            col('trip_id').isNotNull() &
            col('route_id').isNotNull()
        )
        .dropDuplicates(['trip_id'])
    )  

    dim_trips = add_gold_metadata(dim_trips)

    print(f'Dim_trips count: {dim_trips.count()}')
    dim_trips.write.mode('overwrite').option('overwriteSchema', 'true').format('delta').saveAsTable('gold.dim_trips')

    return dim_trips
        