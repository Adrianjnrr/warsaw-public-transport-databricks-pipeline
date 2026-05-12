from pyspark.sql.functions import col, current_timestamp, lit
from uuid import uuid4
from utils.delta_merge import merge_delta_table


def load_bronze(spark, config):
    datasets = config['datasets']
    file_path = config['file_path']

    bronze = {}
    pipeline_run_id = str(uuid4())

    merge_key = {
        'routes': 'route_id',
        'stops': 'stop_id',
        'trips': 'trip_id'
    }

    for name, file in datasets.items():
        full_file_path = f'{file_path}/{file}'

        try:
            already_processed = spark.sql(f"""
                SELECT COUNT(*) AS count
                FROM control.processed_file
                WHERE file_path = '{full_file_path}'
                  AND status = 'success'
            """).collect()[0]["count"]

            if already_processed > 0:
                print(f"Skipping {name}: has already processed")
                continue

            df = (
                spark.read
                .format('csv')
                .option('header', 'true')
                .option('inferSchema', 'true')
                .load(full_file_path)
                .withColumn('_source_file', col('_metadata.file_path'))
                .withColumn('_ingest_timestamp', current_timestamp())
                .withColumn('_pipeline_run_id', lit(pipeline_run_id))
            )

            print(f"Bronze {name} count: {df.count()}")

            if name in merge_key:
                if spark.catalog.tableExists(f"bronze.{name}"):
                    merge_delta_table(
                        spark=spark,
                        source_df=df,
                        target_table=f"bronze.{name}",
                        merge_key=merge_key[name]
                    )
                    print(f"Merged into bronze.{name}")
                else:
                    df.write.mode('overwrite') \
                        .option('overwriteSchema', 'true') \
                        .format('delta') \
                        .saveAsTable(f"bronze.{name}")

                    print(f"Created bronze.{name}")
            else:
                df.write.mode('overwrite') \
                    .option('overwriteSchema', 'true') \
                    .format('delta') \
                    .saveAsTable(f"bronze.{name}")

            spark.sql(f"""
                INSERT INTO control.processed_file
                VALUES (
                    '{name}',
                    '{full_file_path}',
                    current_timestamp(),
                    '{pipeline_run_id}',
                    'SUCCESS',
                    NULL
                )
            """)

            bronze[name] = df

        except Exception as e:
            error_message = str(e).replace("'", "")

            spark.sql(f"""
                INSERT INTO control.processed_file
                VALUES (
                    '{name}',
                    '{full_file_path}',
                    current_timestamp(),
                    '{pipeline_run_id}',
                    'failed',
                    '{error_message}'
                )
            """)

            raise

    return bronze