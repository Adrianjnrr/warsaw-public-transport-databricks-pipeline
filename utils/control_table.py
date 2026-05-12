def create_control_table(spark):
    spark.sql('create schema if not exists control')

    spark.sql("""
              create table if not exists control.processed_file (
                  datasets string,
                  file_path string,
                  processed_at timestamp,
                  pipeline_run_id string,
                  status string
              )
              using delta
              """)