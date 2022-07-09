# encoding: utf-8
# Date: 2022/7/9 08:47

__author__ = 'yudan.chen'

from pyflink.common import Row
from pyflink.table import AggregateFunction, DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import call
from pyflink.table.udf import udaf
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble


class WeightedAvg(AggregateFunction):

    def create_accumulator(self):
        # Row(sum, count)
        return Row(0, 0)

    def get_value(self, accumulator):
        if accumulator[1] == 0:
            return None
        else:
            return accumulator[0] / accumulator[1]

    def accumulate(self, accumulator, value, weight):
        accumulator[0] += value * weight
        accumulator[1] += weight

    def retract(self, accumulator, value, weight):
        accumulator[0] -= value * weight
        accumulator[1] -= weight

    def get_result_type(self):
        return DataTypes.BIGINT()

    def get_accumulator_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("f0", DataTypes.BIGINT()),
            DataTypes.FIELD("f1", DataTypes.BIGINT())])


env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)
# the result type and accumulator type can also be specified in the udaf decorator:
# weighted_avg = udaf(WeightedAvg(), result_type=DataTypes.BIGINT(), accumulator_type=...)
weighted_avg = udaf(WeightedAvg())
t = table_env.from_elements([(1, 2, "Lee"),
                             (3, 4, "Jay"),
                             (5, 6, "Jay"),
                             (7, 8, "Lee")]).alias("value", "count", "name")

# call function "inline" without registration in Table API
result = t.group_by(col("name")).select(weighted_avg(col("value"), col("count")).alias("avg")).execute()
result.print()

# register function
table_env.create_temporary_function("weighted_avg", WeightedAvg())

# call registered function in Table API
result = t.group_by(col("name")).select(call("weighted_avg", col("value"), col("count")).alias("avg")).execute()
result.print()

# register table
table_env.create_temporary_view("source", t)

# call registered function in SQL
result = table_env.sql_query(
    "SELECT weighted_avg(`value`, `count`) AS avg FROM source GROUP BY name").execute()
result.print()

# use the general Python aggregate function in GroupBy Window Aggregation
tumble_window = Tumble.over(lit(1).hours) \
    .on(col("rowtime")) \
    .alias("w")

result = t.window(tumble_window) \
    .group_by(col('w'), col('name')) \
    .select(col('w').start, col('w').end, weighted_avg(col('value'), col('count'))) \
    .execute()
result.print()