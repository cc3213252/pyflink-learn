# encoding: utf-8
# Date: 2022/7/6 16:22

__author__ = 'yudan.chen'

from pyflink.common import Row
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])


@udf(result_type=DataTypes.ROW([DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("data", DataTypes.STRING())]))
def func1(id: int, data: str) -> Row:
    return Row(id, data * 2)


@udf(result_type=DataTypes.ROW([DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("data", DataTypes.STRING())]))
def func2(data: Row) -> Row:
    return Row(data.id, data.data * 2)


# the input columns are specified as the inputs
#table.map(func1(col('id'), col('data'))).execute().print()

table.map(func2).execute().print()