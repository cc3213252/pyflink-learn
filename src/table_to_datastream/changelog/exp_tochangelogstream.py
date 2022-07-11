# encoding: utf-8
# Date: 2022/7/11 16:46

__author__ = 'yudan.chen'

from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessFunction
from pyflink.table import DataTypes, StreamTableEnvironment, Schema
from pyflink.table.expressions import col

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# create Table with event-time
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

# convert to DataStream in the simplest and most general way possible (no event-time)
simple_table = t_env.from_elements([Row("Alice", 12), Row("Alice", 2), Row("Bob", 12)],
                                  DataTypes.ROW([DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("score", DataTypes.INT())]))

simple_table = simple_table.group_by(col('name')).select(col('name'), col('score').sum)

t_env.to_changelog_stream(simple_table).print()

env.execute()

# === EXAMPLE 2 ===

# convert to DataStream in the simplest and most general way possible (with event-time)

ds = t_env.to_changelog_stream(table)

# since `event_time` is a single time attribute in the schema, it is set as the
# stream record's timestamp by default; however, at the same time, it remains part of the Row


class MyProcessFunction(ProcessFunction):
    def process_element(self, row, ctx):
        print(row)
        assert ctx.timestamp() == row.event_time.to_epoch_milli()


ds.process(MyProcessFunction())
env.execute()

# === EXAMPLE 3 ===

# convert to DataStream but write out the time attribute as a metadata column which means
# it is not part of the physical schema anymore

ds = t_env.to_changelog_stream(
    table,
    Schema.new_builder()
        .column("name", "STRING")
        .column("score", "INT")
        .column_by_metadata("rowtime", "TIMESTAMP_LTZ(3)")
        .build())


class MyProcessFunction(ProcessFunction):
    def process_element(self, row, ctx):
        print(row)
        print(ctx.timestamp())


ds.process(MyProcessFunction())
env.execute()