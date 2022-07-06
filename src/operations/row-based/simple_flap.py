# encoding: utf-8
# Date: 2022/7/6 16:38

__author__ = 'yudan.chen'

from pyflink.common import Row
from pyflink.table.udf import udtf
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1, 'Hi,Flink'), (2, 'Hello')], ['id', 'data'])


@udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
def split(x: Row) -> Row:
    for s in x.data.split(","):
        yield x.id, s


# use split in `flat_map`
# table.flat_map(split).execute().print()

table.join_lateral(split.alias('a', 'b')).execute().print()