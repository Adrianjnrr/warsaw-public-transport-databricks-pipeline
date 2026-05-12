def validate_not_null_metadata(spark, table_name, column_name):

    null_count = spark.sql(f"""
        SELECT COUNT(*) AS null_count
        FROM {table_name}
        WHERE {column_name} IS NULL
    """).collect()[0]['null_count']

    if null_count > 0:
        raise Exception(
            f"{table_name}.{column_name} has {null_count} null values"
        )

    print(f"{table_name}.{column_name} validation passed")