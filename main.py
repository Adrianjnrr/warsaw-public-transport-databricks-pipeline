from bronze.bronze_ingestion import load_bronze
from jobs.run_silver import run_silver
from jobs.run_gold import run_gold
from validation.fact_validation import validate_fact_stop, validate_fact_trip
from validation.metadata_validation import validate_not_null_metadata
from validation.table_validation import validate_table_not_empty, validate_not_null,validate_unique_key
from utils.control_table import create_control_table
from utils.config_loader import load_config
from databricks.connect import DatabricksSession

def main():
    spark = DatabricksSession.builder.getOrCreate()
    spark.sql("create schema if not exists bronze")
    spark.sql("create schema if not exists silver")
    spark.sql("create schema if not exists gold")



    config = load_config()
    ## Bronze ingestion

    print('Preparing control table')
    create_control_table(spark)
    
    print("Running bronze ingestion...")
    load_bronze(spark, config)

    ## Silver Tables
    print("Running silver tables...")
    run_silver(spark)

    # Gold Table
    print("Running gold tables...")
    run_gold(spark)

    # Fact validation
    print("Validating gold tables...")
    validate_fact_stop(spark)
    validate_fact_trip(spark)

    print('Validating metadata for silver_tables')

    validate_not_null_metadata(spark, "silver.routes", "_silver_processed_at")
    validate_not_null_metadata(spark, "silver.stops", "_silver_processed_at")
    validate_not_null_metadata(spark, "silver.stop_times", "_silver_processed_at")
    validate_not_null_metadata(spark, "silver.trips", "_silver_processed_at")

    print('Validating metadata for gold_tables')

    validate_not_null_metadata(spark, "gold.dim_routes", "_gold_updated_at")
    validate_not_null_metadata(spark, "gold.dim_stops", "_gold_updated_at")
    validate_not_null_metadata(spark, "gold.fact_stop_times", "_gold_updated_at")
    validate_not_null_metadata(spark, "gold.dim_trips", "_gold_updated_at")

    print('Running table Validation....')

    validate_table_not_empty(spark, 'silver.routes')
    validate_table_not_empty(spark, 'silver.stops')
    validate_table_not_empty(spark, 'silver.stop_times')
    validate_table_not_empty(spark, 'silver.trips')

    validate_table_not_empty(spark, 'gold.dim_routes')
    validate_table_not_empty(spark, 'gold.dim_stops')
    validate_table_not_empty(spark, 'gold.fact_stop_times')
    validate_table_not_empty(spark, 'gold.dim_trips')

    validate_not_null(spark, 'silver.routes', 'route_id')
    validate_not_null(spark, 'silver.stops', 'stop_id')
    validate_not_null(spark, 'silver.stop_times', 'trip_id')
    validate_not_null(spark, 'silver.stop_times', 'stop_id')
    validate_not_null(spark, 'silver.trips', 'trip_id')

    validate_not_null(spark, 'gold.fact_stop_times', 'stop_id')
    validate_not_null(spark, 'gold.dim_stops', 'stop_id')
    validate_not_null(spark, 'gold.dim_trips', 'trip_id')
    validate_not_null(spark, 'gold.dim_routes', 'route_id')
    validate_not_null(spark, 'gold.fact_stop_times', 'trip_id')

    validate_unique_key(spark, 'silver.routes', 'route_id')
    validate_unique_key(spark, 'silver.stops', 'stop_id')
    validate_unique_key(spark, 'silver.trips', 'trip_id')

    validate_unique_key(spark, 'gold.dim_routes', 'route_id')
    validate_unique_key(spark, 'gold.dim_stops', 'stop_id')
    validate_unique_key(spark, 'gold.dim_trips', 'trip_id')


    print('pipeline completed successfully')

if __name__ == "__main__":
   main()