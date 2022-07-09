# encoding: utf-8
# Date: 2022/7/8 13:54

__author__ = 'yudan.chen'

from pyflink.table.expressions import call, col
from pyflink.table import TableEnvironment, EnvironmentSettings


settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(settings)

# register the Java function
table_env.create_java_temporary_function("hash_code", "src.udf.HashCode")

# use the Java function in Python Table API
my_table.select(call('hash_code', col("string")))

# use the Java function in SQL API
table_env.sql_query("SELECT string, bigint, hash_code(string) FROM MyTable")