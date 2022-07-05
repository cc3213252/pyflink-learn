# encoding: utf-8
# Date: 2022/7/5 14:17

__author__ = 'yudan.chen'

from pyflink.table import EnvironmentSettings, TableEnvironment

# use a stream TableEnvironment to execute the queries
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# prepare source tables and sink tables
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.create_temporary_view("simple_source", table)
table_env.execute_sql("""
    CREATE TABLE first_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")
table_env.execute_sql("""
    CREATE TABLE second_sink_table (
        id BIGINT, 
        data VARCHAR
    ) WITH (
        'connector' = 'print'
    )
""")

# create a statement set
statement_set = table_env.create_statement_set()

# emit the "table" object to the "first_sink_table"
statement_set.add_insert("first_sink_table", table)

# emit the "simple_source" to the "second_sink_table" via a insert sql query
statement_set.add_insert_sql("INSERT INTO second_sink_table SELECT * FROM simple_source")

# execute the statement set
statement_set.execute().wait()