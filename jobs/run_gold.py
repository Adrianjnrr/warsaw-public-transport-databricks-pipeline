from gold.route import build_dim_route
from gold.stop import build_dim_stops
from gold.stop_times import build_fact_stop_time
from gold.trip import build_dim_trips


def run_gold(spark):
    build_dim_route(spark)
    build_dim_stops(spark)
    build_fact_stop_time(spark)
    build_dim_trips(spark)




