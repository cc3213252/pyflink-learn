# encoding: utf-8
# Date: 2022/7/9 07:46

__author__ = 'yudan.chen'

from pyflink.table.expressions import call, col
from pyflink.table import TableEnvironment, EnvironmentSettings

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)
my_table = ...  # type: Table, table schema: [a: String]

# Register the java function.
table_env.create_java_temporary_function("split", "my.java.function.Split")

# Use the table function in the Python Table API. "alias" specifies the field names of the table.
my_table.join_lateral(call('split', col('a')).alias("word", "length")).select(col('a'), col('word'), col('length'))
my_table.left_outer_join_lateral(call('split', col('a')).alias("word", "length")).select(col('a'), col('word'), col('length'))

# Register the python function.

# Use the table function in SQL with LATERAL and TABLE keywords.
# CROSS JOIN a table function (equivalent to "join" in Table API).
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
# LEFT JOIN a table function (equivalent to "left_outer_join" in Table API).
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")