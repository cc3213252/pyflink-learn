# encoding: utf-8
# Date: 2022/7/11 10:10

__author__ = 'yudan.chen'

from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udaf
from pyflink.table.window import Tumble


@udaf(result_type=DataTypes.FLOAT(), func_type="pandas")
def mean_udaf(v):
    return v.mean()


settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(settings)

my_table = ...  # type: Table, table schema: [a: String, b: BigInt, c: BigInt]

# use the vectorized Python aggregate function in GroupBy Aggregation
my_table.group_by(col('a')).select(col('a'), mean_udaf(col('b')))


# use the vectorized Python aggregate function in GroupBy Window Aggregation
tumble_window = Tumble.over(lit(1).hours) \
            .on(col("rowtime")) \
            .alias("w")

my_table.window(tumble_window) \
    .group_by(col("w")) \
    .select(col('w').start, col('w').end, mean_udaf(col('b')))

# use the vectorized Python aggregate function in Over Window Aggregation
table_env.create_temporary_function("mean_udaf", mean_udaf)
table_env.sql_query("""
    SELECT a,
        mean_udaf(b)
        over (PARTITION BY a ORDER BY rowtime
        ROWS BETWEEN UNBOUNDED preceding AND UNBOUNDED FOLLOWING)
    FROM MyTable""")