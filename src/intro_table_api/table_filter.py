# encoding: utf-8
# Date: 2022/7/5 13:34

__author__ = 'yudan.chen'

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# use a stream TableEnvironment to execute the queries
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.execute_sql("""CREATE TEMPORARY TABLE Orders (
  a int,
  b STRING,
  c int
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '100'
)
""")


# scan registered Orders table
orders = table_env.from_path("Orders")
# compute revenue for all customers from France
# table_result = orders \
#     .filter(col('a') != col('c')) \
#     .group_by(col('a'), col('b')) \
#     .select(col('a'), col('b'), col('c').sum.alias('revSum'))

# 用sql实现
table_result = table_env.sql_query("select a, b, sum(c) as revSum from Orders where a <> c group by a, b")

table_result.execute().print()