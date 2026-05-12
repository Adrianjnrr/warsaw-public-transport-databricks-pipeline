from pyspark.sql.functions import col, expr, split
from utils.metadata import add_silver_metadata

def build_stop_times_silver(spark):
     stop_times = spark.table('bronze.stop_times')
     stop_times_silver = (
         stop_times
         .select(
             col('trip_id').cast('string'),
             col('stop_id').cast('string'),
             col('arrival_time'),
             col('departure_time'),
             col('pickup_type'),
             col('stop_sequence')
         )
         .filter(
             col('trip_id').isNotNull() &
             col('stop_id').isNotNull() &
             col('stop_sequence').isNotNull()
         )
         
         .withColumn("arrival_seconds",
         split(col("arrival_time"), ":")[0].cast("int") * 3600 +
         split(col("arrival_time"), ":")[1].cast("int") * 60 +
         split(col("arrival_time"), ":")[2].cast("int")
        )
         .withColumn("departure_seconds",
        split(col("departure_time"), ":")[0].cast("int") * 3600 +
        split(col("departure_time"), ":")[1].cast("int") * 60 +
        split(col("departure_time"), ":")[2].cast("int")
    )
        .dropDuplicates(['trip_id', 'stop_id', 'stop_sequence'])
         
     )
     stop_times_silver = add_silver_metadata(stop_times_silver)

     print(f"Silver Stop_times count: {stop_times_silver.count()}")

     
     
     stop_times_silver.write.mode('overwrite').option('overwriteSchema', 'true').format('delta').saveAsTable('silver.stop_times')
     return stop_times_silver