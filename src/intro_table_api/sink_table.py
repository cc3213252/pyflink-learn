# encoding: utf-8
# Date: 2022/7/5 14:11

__author__ = 'yudan.chen'

from pyflink.table import EnvironmentSettings, TableEnvironment

# use a stream TableEnvironment to execute the queries
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.execute_sql("""
    CREATE TABLE sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
# table.execute_insert("sink_table").wait()

table_env.create_temporary_view("table_source", table)
table_env.execute_sql("INSERT INTO sink_table SELECT * FROM table_source").wait()