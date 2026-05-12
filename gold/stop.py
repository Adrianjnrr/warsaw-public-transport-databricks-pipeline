from pyspark.sql.functions import col
from utils.metadata import add_gold_metadata

def build_dim_stops(spark):
    gold_stops = spark.table('silver.stops')
    dim_stops = (
        gold_stops
        .select(
            col('stop_id').cast('string'),
            col('stop_name')
        )
        .filter(
            col('stop_id').isNotNull()
        )
        .dropDuplicates(['stop_id'])
    )

    dim_stops = add_gold_metadata(dim_stops)

    print(f'Dim_stops count: {dim_stops.count()}')

    dim_stops.write.mode('overwrite').option('overwriteSchema', 'true').format('delta').saveAsTable('gold.dim_stops')
    return dim_stops
