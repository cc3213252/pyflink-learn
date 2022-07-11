# encoding: utf-8
# Date: 2022/7/11 13:34

__author__ = 'yudan.chen'

table_env.from_path("input_table").execute_insert("output_table")

table_env.execute_sql("INSERT INTO output_table SELECT * FROM input_table")

table_env.create_statement_set() \
    .add_insert("output_table", input_table) \
    .add_insert("output_table2", input_table) \
    .execute()

table_env.create_statement_set() \
    .add_insert_sql("INSERT INTO output_table SELECT * FROM input_table") \
    .add_insert_sql("INSERT INTO output_table2 SELECT * FROM input_table") \
    .execute()

# execute with implicit local sink
table_env.from_path("input_table").execute().print()

table_env.execute_sql("SELECT * FROM input_table").print()