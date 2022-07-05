# encoding: utf-8
# Date: 2022/7/5 14:26

__author__ = 'yudan.chen'

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table1 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table2 = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table = table1 \
    .where(col("data").like('H%')) \
    .union_all(table2)
print(table.explain())