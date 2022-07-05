# encoding: utf-8
# Date: 2022/7/5 15:57

__author__ = 'yudan.chen'

# specify table program
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble
from pyflink.table import *

# environment configuration
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
orders = t_env.from_path("Orders")  # schema (a, b, c, rowtime)

result = orders.filter(col("a").is_not_null & col("b").is_not_null & col("c").is_not_null) \
               .select(col("a").lower_case.alias('a'), col("b"), col("rowtime")) \
               .window(Tumble.over(lit(1).hour).on(col("rowtime")).alias("hourly_window")) \
               .group_by(col('hourly_window'), col('a')) \
               .select(col('a'), col('hourly_window').end.alias('hour'), col("b").avg.alias('avg_billing_amount'))

result.execute().print()
