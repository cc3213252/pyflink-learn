# encoding: utf-8
# Date: 2022/7/5 16:46

__author__ = 'yudan.chen'

from pyflink.table.expressions import col, lit, UNBOUNDED_RANGE
from pyflink.table.window import Over, Tumble
from pyflink.table import *


t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_batch_mode())

# register Orders table and Result table sink in table environment
source_data_path = "source.txt"
source_ddl = f"""
        create table Orders(
            a VARCHAR,
            b BIGINT,
            c BIGINT,
            rowtime TIMESTAMP(3),
            WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
        ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{source_data_path}'
        )
        """
t_env.execute_sql(source_ddl)
orders = t_env.from_path("Orders")
# Distinct aggregation on group by
group_by_distinct_result = orders.group_by(col("a")) \
                                 .select(col("a"), col("b").sum.distinct.alias('d'))

# Distinct aggregation on time window group by
group_by_window_distinct_result = orders.window(Tumble.over(lit(5).minutes).on(col("rowtime")).alias("w")) \
    .group_by(col("a"), col('w')) \
    .select(col("a"), col("b").sum.distinct.alias('d'))

# Distinct aggregation on over window
result = orders.over_window(Over.partition_by(col("a"))
                                .order_by(col("rowtime"))
                                .preceding(UNBOUNDED_RANGE)
                                .alias("w")) \
    .select(col("a"), col("b").avg.distinct.over(col('w')), col("b").max.over(col('w')), col("b").min.over(col('w')))

result.execute().print()
