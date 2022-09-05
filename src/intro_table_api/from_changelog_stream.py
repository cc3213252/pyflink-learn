# encoding: utf-8
# Date: 2022/9/2 13:33

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
        type_info=Types.ROW([Types.STRING(),Types.INT()]))

# interpret the DataStream as a Table
table = t_env.from_changelog_stream(ds)


# register the table under a name and perform an aggregation
t_env.create_temporary_view("InputTable", table)
t_env.execute_sql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0").print()

# prints:
# +----+--------------------------------+-------------+
# | op |                           name |       score |
# +----+--------------------------------+-------------+
# | +I |                            Bob |           5 |
# | +I |                          Alice |          12 |
# | -D |                          Alice |          12 |
# | +I |                          Alice |         100 |
# +----+--------------------------------+-------------+

# === EXAMPLE 2 ===

# interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)

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
t_env.execute_sql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0").print()
