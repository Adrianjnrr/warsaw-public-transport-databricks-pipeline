from pyspark.sql.functions import *

Route_Schema = StructType([
    StructField('route_id', StringType(), False),
    StructField('agency_id', StringType(), True),
    StructField('route_short_name', StringType(), False),
    StructField('route_long_name', StringType(), True)
])