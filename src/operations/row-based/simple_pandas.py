# encoding: utf-8
# Date: 2022/7/6 17:17

__author__ = 'yudan.chen'

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udaf


env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

t = table_env.from_elements([(1, 2), (2, 1), (1, 3)], ['a', 'b'])

pandas_udaf = udaf(lambda pd: (pd.b.mean(), pd.b.max()),
                   result_type=DataTypes.ROW(
                       [DataTypes.FIELD("a", DataTypes.FLOAT()),
                        DataTypes.FIELD("b", DataTypes.INT())]),
                   func_type="pandas")
t.aggregate(pandas_udaf.alias("a", "b")) \
 .select(col('a'), col('b')).execute().print()
