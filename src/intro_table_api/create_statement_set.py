# encoding: utf-8
# Date: 2022/9/2 13:48

__author__ = 'yudan.chen'

from pyflink.common import Encoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink
from pyflink.table import StreamTableEnvironment, TableDescriptor, Schema, DataTypes

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

statement_set = table_env.create_statement_set()

# create some source
source_descriptor = TableDescriptor.for_connector("datagen") \
    .option("number-of-rows", "3") \
    .schema(
    Schema.new_builder()
        .column("my_col", DataTypes.INT())
        .column("my_other_col", DataTypes.BOOLEAN())
        .build()) \
    .build()

# create some sink
sink_descriptor = TableDescriptor.for_connector("print").build()

# add a pure Table API pipeline
table_from_source = table_env.from_descriptor(source_descriptor)
statement_set.add_insert(sink_descriptor, table_from_source)

# use table sinks for the DataStream API pipeline
data_stream = env.from_collection([1, 2, 3])
table_from_stream = table_env.from_data_stream(data_stream)
statement_set.add_insert(sink_descriptor, table_from_stream)

# define other DataStream API parts
env.from_collection([4, 5, 6]).add_sink(StreamingFileSink
          .for_row_format('/tmp/output', Encoder.simple_string_encoder())
          .build())

# use DataStream API to submit the pipelines
env.execute()