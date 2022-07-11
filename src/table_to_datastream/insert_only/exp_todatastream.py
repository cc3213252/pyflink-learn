# encoding: utf-8
# Date: 2022/7/11 14:56

__author__ = 'yudan.chen'

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

t_env.execute_sql(
    "CREATE TABLE GeneratedTable "
    + "("
    + "  name STRING,"
    + "  score INT,"
    + "  event_time TIMESTAMP_LTZ(3),"
    + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
    + ")"
    + "WITH ('connector'='datagen')")

table = t_env.from_path("GeneratedTable")


# === EXAMPLE 1 ===

# use the default conversion to instances of Row

# since `event_time` is a single rowtime attribute, it is inserted into the DataStream
# metadata and watermarks are propagated

ds = t_env.to_data_stream(table)
ds.print()