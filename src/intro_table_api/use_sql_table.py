# encoding: utf-8
# Date: 2022/7/5 13:31

__author__ = 'yudan.chen'

from pyflink.table import EnvironmentSettings, TableEnvironment

# use a stream TableEnvironment to execute the queries
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.execute_sql("""
    CREATE TABLE sql_source (
        id BIGINT, 
        data TINYINT 
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind'='sequence',
        'fields.id.start'='1',
        'fields.id.end'='4',
        'fields.data.kind'='sequence',
        'fields.data.start'='4',
        'fields.data.end'='7'
    )
""")

# convert the sql table to Table API table
table = table_env.from_path("sql_source")

# or create the table from a sql query
# table = table_env.sql_query("SELECT * FROM sql_source")

# emit the table
table.execute().print()