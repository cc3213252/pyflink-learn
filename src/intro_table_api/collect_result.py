# encoding: utf-8
# Date: 2022/7/5 14:07

__author__ = 'yudan.chen'

from pyflink.table import EnvironmentSettings, TableEnvironment

# use a stream TableEnvironment to execute the queries
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

source = table_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])

# Get TableResult
table_result = table_env.execute_sql("select a + 1, b, c from %s" % source)

# Traversal result
with table_result.collect() as results:
   for result in results:
       print(result)