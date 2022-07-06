# encoding: utf-8
# Date: 2022/7/6 17:07

__author__ = 'yudan.chen'

from pyflink.common import Row
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import AggregateFunction, udaf


class CountAndSumAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return Row(accumulator[0], accumulator[1])

    def create_accumulator(self):
        return Row(0, 0)

    def accumulate(self, accumulator, row):
        accumulator[0] += 1
        accumulator[1] += row.b

    def retract(self, accumulator, row):
        accumulator[0] -= 1
        accumulator[1] -= row.b

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]

    def get_accumulator_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])


function = CountAndSumAggregateFunction()
agg = udaf(function,
           result_type=function.get_result_type(),
           accumulator_type=function.get_accumulator_type(),
           name=str(function.__class__.__name__))

# aggregate with a python general aggregate function

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)
t = table_env.from_elements([(1, 2), (2, 1), (1, 3)], ['a', 'b'])

result = t.group_by(col('a')) \
    .aggregate(agg.alias("c", "d")) \
    .select(col('a'), col('c'), col('d'))
result.execute().print()
