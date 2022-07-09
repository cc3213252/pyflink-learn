# encoding: utf-8
# Date: 2022/7/7 15:20

__author__ = 'yudan.chen'

from pyflink.table.expressions import call, col
from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.udf import ScalarFunction, udf


class HashCode(ScalarFunction):
    def __init__(self):
      self.factor = 12

    def eval(self, s):
      return hash(s) * self.factor


settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(settings)

hash_code = udf(HashCode(), result_type=DataTypes.BIGINT())

# my_table = table_env.from_elements([('Hi', 1), ('Hello', 2)], ['string', 'bigint'])
# use the Python function in Python Table API
my_table.select(col("string"), col("bigint"), hash_code(col("bigint")), call(hash_code, col("bigint")))

# use the Python function in SQL API
table_env.create_temporary_function("hash_code", udf(HashCode(), result_type=DataTypes.BIGINT()))
table_env.sql_query("SELECT string, bigint, hash_code(bigint) FROM MyTable")

