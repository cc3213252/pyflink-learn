# encoding: utf-8
# Date: 2022/7/5 17:19

__author__ = 'yudan.chen'

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import DataTypes, Row
from pyflink.table.udf import udtf
from pyflink.table.expressions import col

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


# register User-Defined Table Function
@udtf(result_types=[DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])
def split(x):
    return [Row(1, 2, 3)]


# join
orders = t_env.from_path("Orders")
joined_table = orders.join_lateral(split(col('c')).alias("s", "t", "v"))
result = joined_table.select(col('a'), col('b'), col('s'), col('t'), col('v'))
result.execute().print()
