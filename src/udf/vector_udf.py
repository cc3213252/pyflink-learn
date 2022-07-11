# encoding: utf-8
# Date: 2022/7/9 15:15

__author__ = 'yudan.chen'

from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.udf import udf


@udf(result_type=DataTypes.BIGINT(), func_type="pandas")
def add(i, j):
    return i + j


settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(settings)

# my_table = table_env.from_elements([(2, 1), (3, 2)])
# use the vectorized Python scalar function in Python Table API
my_table.select(add(col("bigint"), col("bigint")))

# use the vectorized Python scalar function in SQL API
table_env.create_temporary_function("add", add)
table_env.sql_query("SELECT add(bigint, bigint) FROM MyTable")