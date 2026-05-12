import pytest
from validation.table_validation import validate_not_null
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()

def test_validate_not_null_passes(spark):
    data = [("1",), ("2",), ("3",)]  
    df = spark.createDataFrame(
        data,
        ['route_id']
    )  
    df.createOrReplaceTempView('test_routes')
    
    validate_not_null(
        spark,
        'test_routes',
        'route_id'
    )

def test_validate_not_null_fails(spark):

    data = [("1",), (None,), ("3",)]

    df = spark.createDataFrame(
        data,
        ['route_id']
    )

    df.createOrReplaceTempView('test_routes_null')

    with pytest.raises(Exception):

        validate_not_null(
            spark,
            'test_routes_null',
            'route_id'
        )
