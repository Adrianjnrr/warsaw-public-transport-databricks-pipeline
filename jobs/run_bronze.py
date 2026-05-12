from bronze.bronze_ingestion import load_bronze


def run_bronze_ingestion(config, spark):
    load_bronze(config, spark)


