# encoding: utf-8
# Date: 2022/7/11 14:45

__author__ = 'yudan.chen'

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, StreamTableEnvironment, Schema

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)
ds = env.from_collection([(12, "Alice"), (0, "Bob")], type_info=Types.TUPLE([Types.LONG(), Types.STRING()]))

# === EXAMPLE 1 ===

# register the DataStream as view "MyView" in the current session
# all columns are derived automatically

t_env.create_temporary_view("MyView", ds)

t_env.from_path("MyView").print_schema()

# === EXAMPLE 2 ===

# register the DataStream as view "MyView" in the current session,
# provide a schema to adjust the columns similar to `fromDataStream`

# in this example, the derived NOT NULL information has been removed

t_env.create_temporary_view(
    "MyView2",
    ds,
    Schema.new_builder()
        .column("f0", "BIGINT")
        .column("f1", "STRING")
        .build())

t_env.from_path("MyView2").print_schema()

# === EXAMPLE 3 ===

# use the Table API before creating the view if it is only about renaming columns

t_env.create_temporary_view(
    "MyView3",
    t_env.from_data_stream(ds).alias("id", "name"))

t_env.from_path("MyView3").print_schema()
