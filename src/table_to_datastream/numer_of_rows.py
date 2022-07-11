# encoding: utf-8
# Date: 2022/7/11 13:55

__author__ = 'yudan.chen'


from pyflink.table import TableDescriptor, Schema, DataTypes
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env, EnvironmentSettings.in_batch_mode())

table = table_env.from_descriptor(
    TableDescriptor.for_connector("datagen")
        .option("number-of-rows", "10")
        .schema(
        Schema.new_builder()
            .column("uid", DataTypes.TINYINT())
            .column("payload", DataTypes.STRING())
            .build())
        .build())

# convert the Table to a DataStream and further transform the pipeline
collect = table_env.to_data_stream(table) \
    .key_by(lambda r: r[0]) \
    .map(lambda r: "My custom operator: " + r[1]) \
    .execute_and_collect()

for c in collect:
    print(c)