from pyspark.sql.functions import col
from utils.metadata import add_silver_metadata

def build_trips_silver(spark):
    trips = spark.table('bronze.trips')

    trip_silver =(
        trips
        .select(
            col('route_id').cast('string'),
            col('trip_id').cast('string')
        )
        .filter(
            col('trip_id').isNotNull() &
            col('route_id').isNotNull()
        )
        .dropDuplicates(['trip_id'])
    )

    trip_silver = add_silver_metadata(trip_silver)

    print(f"Silver Trip count: {trip_silver.count()}")
    
    trip_silver.write.mode('overwrite').option('overwriteSchema', 'true').format('delta').saveAsTable('silver.trips')
    return trip_silver