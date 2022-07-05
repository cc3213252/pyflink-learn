# encoding: utf-8
# Date: 2022/7/5 15:45

__author__ = 'yudan.chen'

from pyflink.table import *
from pyflink.table.expressions import col

# environment configuration
t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_batch_mode())

# register Orders table and Result table sink in table environment
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

sink_ddl = f"""
    create table `Result`(
        a VARCHAR,
        cnt BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{result_data_path}'
    )
    """
t_env.execute_sql(sink_ddl)

# specify table program
orders = t_env.from_path("Orders")  # schema (a, b, c, rowtime)

orders.group_by(col("a")).select(col("a"), col("b").count.alias('cnt')).execute_insert("Result").wait()