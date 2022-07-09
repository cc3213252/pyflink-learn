# encoding: utf-8
# Date: 2022/7/9 07:41

__author__ = 'yudan.chen'

from pyflink.table.expressions import col
from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.udf import TableFunction, udtf


class Split(TableFunction):
    def eval(self, string):
        for s in string.split(" "):
            yield s, len(s)


env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)
my_table = ...  # type: Table, table schema: [a: String]

# register the Python Table Function
split = udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()])

# use the Python Table Function in Python Table API
my_table.join_lateral(split(col("a")).alias("word", "length"))
my_table.left_outer_join_lateral(split(col("a")).alias("word", "length"))

# use the Python Table function in SQL API
table_env.create_temporary_function("split", udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()]))
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")
