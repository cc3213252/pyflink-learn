# encoding: utf-8
# Date: 2022/9/1 14:27

__author__ = 'yudan.chen'

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.typeinfo import Types

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# create a DataStream
ds = env.from_collection(["Alice", "Bob", "John"], Types.STRING())

# interpret the insert-only DataStream as a Table
t = t_env.from_data_stream(ds)

# register the Table object as a view and query it
t_env.create_temporary_view("InputTable", t)
res_table = t_env.sql_query("SELECT UPPER(f0) FROM InputTable")

# interpret the insert-only Table as a DataStream again
res_ds = t_env.to_data_stream(res_table)

# add a printing sink and execute in DataStream API
res_ds.print()

env.execute()