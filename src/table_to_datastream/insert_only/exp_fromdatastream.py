# encoding: utf-8
# Date: 2022/7/11 14:20

__author__ = 'yudan.chen'

from pyflink.common.time import Instant
from pyflink.common.types import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, Schema

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)
ds = env.from_collection([
    Row("Alice", 12, Instant.of_epoch_milli(1000)),
    Row("Bob", 5, Instant.of_epoch_milli(1001)),
    Row("Alice", 10, Instant.of_epoch_milli(1002))],
    type_info=Types.ROW_NAMED(['name', 'score', 'event_time'], [Types.STRING(), Types.INT(), Types.INSTANT()]))

# === EXAMPLE 1 ===

# derive all physical columns automatically

table = t_env.from_data_stream(ds)
table.print_schema()

# === EXAMPLE 2 ===

# derive all physical columns automatically
# but add computed columns (in this case for creating a proctime attribute column)
# 如何增加一个处理时间字段

table = t_env.from_data_stream(
    ds,
    Schema.new_builder()
        .column_by_expression("proc_time", "PROCTIME()")
        .build())
table.print_schema()

# === EXAMPLE 3 ===

# derive all physical columns automatically
# but add computed columns (in this case for creating a rowtime attribute column)
# and a custom watermark strategy

table = t_env.from_data_stream(
          ds,
          Schema.new_builder()
              .column_by_expression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
              .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
              .build())
table.print_schema()

# === EXAMPLE 4 ===

# derive all physical columns automatically
# but access the stream record's timestamp for creating a rowtime attribute column
# also rely on the watermarks generated in the DataStream API

# we assume that a watermark strategy has been defined for `dataStream` before
# (not part of this example)
table = t_env.from_data_stream(
        ds,
        Schema.new_builder()
            .column_by_metadata("rowtime", "TIMESTAMP_LTZ(3)")
            .watermark("rowtime", "SOURCE_WATERMARK()")
            .build())
table.print_schema()

# === EXAMPLE 5 ===

# define physical columns manually
# in this example,
#   - we can reduce the default precision of timestamps from 9 to 3
#   - we also project the columns and put `event_time` to the beginning
# 清晰自定义类型

table = t_env.from_data_stream(
        ds,
        Schema.new_builder()
            .column("event_time", "TIMESTAMP_LTZ(3)")
            .column("name", "STRING")
            .column("score", "INT")
            .watermark("event_time", "SOURCE_WATERMARK()")
            .build())
table.print_schema()