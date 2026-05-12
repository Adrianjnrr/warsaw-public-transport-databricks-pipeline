from pyspark.sql.functions import col
from utils.metadata import add_gold_metadata

def build_dim_route(spark):
    gold_routes = spark.table('silver.routes')
    dim_routes = (
        gold_routes
        .select(
            col('route_id').cast('string'),
            col('route_short_name'),
            col('route_type')
        )
        .filter(
            col('route_id').isNotNull()
        )
        .dropDuplicates(['route_id'])
    )

    dim_routes = add_gold_metadata(dim_routes)
    
    print(f"dim_routes count: {dim_routes.count()}")
    dim_routes.write.mode('overwrite').option('overwriteSchema', 'true').format('delta').saveAsTable('gold.dim_routes')
    
    return dim_routes
