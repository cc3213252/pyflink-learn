# encoding: utf-8
# Date: 2022/7/12 17:23

__author__ = 'yudan.chen'

from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer

env = StreamExecutionEnvironment.get_execution_environment()
# the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
env.add_jars("file:///path/to/flink-sql-connector-kafka.jar")

deserialization_schema = JsonRowDeserializationSchema.builder() \
    .type_info(type_info=Types.ROW([Types.INT(), Types.STRING()])).build()

kafka_consumer = FlinkKafkaConsumer(
    topics='test_source_topic',
    deserialization_schema=deserialization_schema,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

ds = env.add_source(kafka_consumer)