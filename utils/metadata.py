from pyspark.sql.functions import current_timestamp

def add_silver_metadata(df):
    return df.withColumn(
        '_silver_processed_at', current_timestamp()
    )


def add_gold_metadata(df):
    return df.withColumn(
        '_gold_updated_at', current_timestamp()
    )