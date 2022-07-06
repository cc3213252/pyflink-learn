# encoding: utf-8
# Date: 2022/7/6 17:21

__author__ = 'yudan.chen'

from pyflink.common import Row
from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.udf import udtaf, TableAggregateFunction


class Top2(TableAggregateFunction):

    def emit_value(self, accumulator):
        yield Row(accumulator[0])
        yield Row(accumulator[1])

    def create_accumulator(self):
        return [None, None]

    def accumulate(self, accumulator, row):
        if row.a is not None:
            if accumulator[0] is None or row.a > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = row.a
            elif accumulator[1] is None or row.a > accumulator[1]:
                accumulator[1] = row.a

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT())])


env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)
# the result type and accumulator type can also be specified in the udtaf decorator:
# top2 = udtaf(Top2(), result_type=DataTypes.ROW([DataTypes.FIELD("a", DataTypes.BIGINT())]), accumulator_type=DataTypes.ARRAY(DataTypes.BIGINT()))
top2 = udtaf(Top2())
t = table_env.from_elements([(1, 'Hi', 'Hello'),
                             (3, 'Hi', 'hi'),
                             (5, 'Hi2', 'hi'),
                             (7, 'Hi', 'Hello'),
                             (2, 'Hi', 'Hello')],
                            ['a', 'b', 'c'])

# call function "inline" without registration in Table API
result = t.group_by(col('b')).flat_aggregate(top2).select(col('*')).execute().print()
