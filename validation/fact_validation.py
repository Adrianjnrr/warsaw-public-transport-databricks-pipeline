def validate_fact_stop(spark):
    fact = spark.table('gold.fact_stop_times')
    dim_stop = spark.table('gold.dim_stops')

    invalid_stops = fact.join(dim_stop, on ='stop_id', how = 'left_anti')

    count = invalid_stops.count()

    if count > 0:
        raise ValueError(f"Invalid stop_id found: {count}")
    print('stop_id integrity passed')



def validate_fact_trip(spark):
    fact = spark.table('gold.fact_stop_times')
    dim_trip = spark.table('gold.dim_trips')

    invalid_trips = fact.join(dim_trip, on = 'trip_id', how = 'left_anti')

    count = invalid_trips.count()

    if count > 0:
        raise ValueError(f"Invalid trip_id found {count}")
    print('trip_id integrity passed')






           