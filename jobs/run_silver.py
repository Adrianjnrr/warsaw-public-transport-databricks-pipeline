from silver.route import build_route_silver
from silver.stop import build_stop_silver
from silver.stop_times import build_stop_times_silver
from silver.trip import build_trips_silver


def run_silver(spark):
    build_route_silver(spark)
    build_stop_silver(spark)
    build_stop_times_silver(spark)
    build_trips_silver(spark)
