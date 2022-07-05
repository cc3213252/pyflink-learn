# encoding: utf-8
# Date: 2022/7/5 16:33

__author__ = 'yudan.chen'

from pyflink.table.window import Tumble
from pyflink.table.expressions import lit, col
from pyflink.table import *


t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_batch_mode())
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
result = orders.window(Tumble.over(lit(5).minutes).on(col('rowtime')).alias("w")) \
               .group_by(col('a'), col('w')) \
               .select(col('a'), col('w').start, col('w').end, col('b').sum.alias('d'))
result.execute().print()
