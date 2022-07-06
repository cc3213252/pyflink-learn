# encoding: utf-8
# Date: 2022/7/6 15:07

__author__ = 'yudan.chen'

from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.udf import udf


def map_function(a: Row) -> Row:
    return Row(a.a + 1, a.b * a.b)


# map operation with a python general scalar function
func = udf(map_function, result_type=DataTypes.ROW(
                                     [DataTypes.FIELD("a", DataTypes.BIGINT()),
                                      DataTypes.FIELD("b", DataTypes.BIGINT())]))
table = input.map(func).alias('a', 'b')

# map operation with a python vectorized scalar function
pandas_func = udf(lambda x: x * 2,
                  result_type=DataTypes.ROW([DataTypes.FIELD("a", DataTypes.BIGINT()),
                                             DataTypes.FIELD("b", DataTypes.BIGINT())]),
                  func_type='pandas')

table = input.map(pandas_func).alias('a', 'b')