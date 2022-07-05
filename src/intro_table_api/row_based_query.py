# encoding: utf-8
# Date: 2022/7/4 15:11

__author__ = 'yudan.chen'

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import DataTypes
from pyflink.table.udf import udf
import pandas as pd

# using batch table environment to execute the queries
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

orders = table_env.from_elements([('Jack', 'FRANCE', 10), ('Rose', 'ENGLAND', 30), ('Jack', 'FRANCE', 20)],
                                 ['name', 'country', 'revenue'])

map_function = udf(lambda x: pd.concat([x.name, x.revenue * 10], axis=1),
                   result_type=DataTypes.ROW(
                               [DataTypes.FIELD("name", DataTypes.STRING()),
                                DataTypes.FIELD("revenue", DataTypes.BIGINT())]),
                   func_type="pandas")

orders.map(map_function).execute().print()