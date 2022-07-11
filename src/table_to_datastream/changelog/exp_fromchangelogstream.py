# encoding: utf-8
# Date: 2022/7/11 15:17

__author__ = 'yudan.chen'

from pyflink.common import Row, RowKind
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, StreamTableEnvironment, Schema, ChangelogMode

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# === EXAMPLE 1 ===

# create a changelog DataStream

ds = env.from_collection([
        Row.of_kind(RowKind.INSERT, "Alice", 12),
        Row.of_kind(RowKind.INSERT, "Bob", 5),
        Row.of_kind(RowKind.UPDATE_BEFORE, "Alice", 12),
        Row.of_kind(RowKind.UPDATE_AFTER, "Alice", 100)],
        type_info=Types.ROW([Types.STRING(), Types.INT()]))

# interpret the DataStream as a Table
table = t_env.from_changelog_stream(ds)


# register the table under a name and perform an aggregation
t_env.create_temporary_view("InputTable", table)
t_env.execute_sql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0").print()

# === EXAMPLE 2 ===

# interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)
# 示例2展示了如何通过使用upsert模式将更新消息的数量减少50%来限制传入更改的种类，从而提高效率。通过为toChangelogStream定义一个主键和upsert变更日志管理模式，可以减少结果消息的数量
# create a changelog DataStream
ds = env.from_collection([
        Row.of_kind(RowKind.INSERT, "Alice", 12),
        Row.of_kind(RowKind.INSERT, "Bob", 5),
        Row.of_kind(RowKind.UPDATE_AFTER, "Alice", 100)],
        type_info=Types.ROW([Types.STRING(),Types.INT()]))

# interpret the DataStream as a Table
table = t_env.from_changelog_stream(
        ds,
        Schema.new_builder().primary_key("f0").build(),
        ChangelogMode.upsert())

# register the table under a name and perform an aggregation
t_env.create_temporary_view("InputTable2", table)
t_env.execute_sql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable2 GROUP BY f0").print()
