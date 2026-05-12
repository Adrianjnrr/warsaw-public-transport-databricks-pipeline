def validate_table_not_empty(spark, table_name):
    row_count = spark.table(table_name).count()
    if row_count == 0:
        raise Exception(f"{table_name} is empty")
    print(f"{table_name} not-empty validation passed {row_count} rows")


def validate_not_null(spark, table_name, column_name):
    null_count = spark.sql(f"""
         select count(*) as null_count
         from {table_name}
         where {column_name} is null                
     """).collect()[0]['null_count'] 
    
    if null_count > 0:
        raise Exception(f"{table_name}.{column_name} has {null_count} null value")
    print(f"{table_name}.{column_name} not_null validation passed")

def validate_unique_key(spark, table_name, key_column):
    duplicate_count = spark.sql(f"""
           select count(*)  as duplicate_count
           from(
               select {key_column}
               from {table_name}
               group by {key_column}
               having count(*)>1
            )                  
     """).collect()[0]['duplicate_count']  
     
    if duplicate_count > 0:
        raise Exception(f"{table_name}.{key_column} has duplicate value")
    print(f"{table_name}.{key_column} unique key validation passed") 