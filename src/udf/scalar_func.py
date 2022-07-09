# encoding: utf-8
# Date: 2022/7/8 14:16

__author__ = 'yudan.chen'

from pyflink.table.expressions import call, col
from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.udf import ScalarFunction, udf


class Add(ScalarFunction):
  def eval(self, i, j):
    return i + j


add = udf(Add(), result_type=DataTypes.BIGINT())


# option 2: Python function
@udf(result_type=DataTypes.BIGINT())
def add(i, j):
    return i + j


# option 3: lambda function
add = udf(lambda i, j: i + j, result_type=DataTypes.BIGINT())


# option 4: callable function
class CallableAdd(object):
    def __call__(self, i, j):
      return i + j


add = udf(CallableAdd(), result_type=DataTypes.BIGINT())


# option 5: partial function
def partial_add(i, j, k):
    return i + j + k


add = udf(functools.partial(partial_add, k=1), result_type=DataTypes.BIGINT())


settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(settings)
# register the Python function
table_env.create_temporary_function("add", add)
# use the function in Python Table API
my_table.select(call('add', col('a'), col('b')))

# You can also use the Python function in Python Table API directly
my_table.select(add(col('a'), col('b')))