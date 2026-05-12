def merge_delta_table(spark, source_df, target_table, merge_key):
    source_view = 'source_updates'
    source_df.createOrReplaceTempView(source_view)

    spark.sql(f"""
              merge into {target_table} as target
              using {source_view} as source
              on target.{merge_key} = source.{merge_key}
              when matched then update set *
              when not matched then insert *
              """)