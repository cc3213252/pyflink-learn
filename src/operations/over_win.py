# encoding: utf-8
# Date: 2022/7/5 16:35

__author__ = 'yudan.chen'

from pyflink.table.window import Over
from pyflink.table.expressions import col, UNBOUNDED_RANGE, CURRENT_RANGE
from pyflink.table import *


t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_batch_mode())
source_data_path = "source.txt"
result_data_path = "result"
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
result = orders.over_window(Over.partition_by(col("a")).order_by(col("rowtime"))
                            .preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE)
                            .alias("w")) \
               .select(col("a"), col("b").avg.over(col('w')), col("b").max.over(col('w')), col("b").min.over(col('w')))
result.execute().print()
