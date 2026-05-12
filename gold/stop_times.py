from pyspark.sql.functions import col 
from utils.metadata import add_gold_metadata

def build_fact_stop_time(spark):
    gold_stop_times = spark.table('silver.stop_times')
    fact_stop_times =(
        gold_stop_times
        .select(
            col('trip_id').cast('string'),
            col('stop_id').cast('string'),
            col('arrival_seconds'),
            col('departure_seconds'),
            col('stop_sequence').cast('int'),
            col('pickup_type').cast('string')
        )
        .filter(
            col('trip_id').isNotNull() &
            col('stop_id').isNotNull() &
            col('stop_sequence').isNotNull()
        )
        .dropDuplicates(['trip_id','stop_id','stop_sequence'])

    )
    fact_stop_times = add_gold_metadata(fact_stop_times)
    
    print(f"Fact table count: {fact_stop_times.count()}")
    fact_stop_times.write.mode('overwrite').option("overwriteSchema", "true").format('delta').saveAsTable("gold.fact_stop_times")

    return fact_stop_times