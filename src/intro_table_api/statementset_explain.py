# encoding: utf-8
# Date: 2022/7/5 14:28

__author__ = 'yudan.chen'

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table1 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table2 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.execute_sql("""
    CREATE TABLE print_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'print'
    )
""")
table_env.execute_sql("""
    CREATE TABLE black_hole_sink_table (
        id BIGINT, 
        data VARCHAR 
    ) WITH (
        'connector' = 'blackhole'
    )
""")

statement_set = table_env.create_statement_set()

statement_set.add_insert("print_sink_table", table1.where(col("data").like('H%')))
statement_set.add_insert("black_hole_sink_table", table2)

print(statement_set.explain())