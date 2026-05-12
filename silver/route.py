from pyspark.sql.functions import col, current_timestamp
from utils.metadata import add_silver_metadata

def build_route_silver(spark):
    routes =spark.table("bronze.routes")
    route_silver =(
        routes
        .select(
            col('route_id').cast('string'),
            col('route_short_name').cast('string'),
            col('route_type').cast('int')
        )
        .filter(col('route_id').isNotNull())
        .dropDuplicates(['route_id'])
    )
    route_silver = add_silver_metadata(route_silver)

    print(f"Silver route count: {route_silver.count()}")

    route_silver.write.mode('overwrite').option('overwriteSchema', 'true').format('delta').saveAsTable('silver.routes')
    return route_silver