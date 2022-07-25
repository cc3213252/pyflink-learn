## datastream to table

table = t_env.from_data_stream(ds, 'a, b, c')

ds = t_env.to_append_stream(table, Types.ROW([Types.INT(), Types.STRING()]))
或者
ds = t_env.to_retract_stream(table, Types.ROW([Types.INT(), Types.STRING()]))

## emit results

1、ds.print()  
2、execute_and_collect  
```python
with ds.execute_and_collect() as results:
    for result in results:
        print(result)
```
3、datastream sink connector  
add_sink只能用在流模式  
```python
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.serialization import JsonRowSerializationSchema

serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
    type_info=Types.ROW([Types.INT(), Types.STRING()])).build()

kafka_producer = FlinkKafkaProducer(
    topic='test_sink_topic',
    serialization_schema=serialization_schema,
    producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

ds.add_sink(kafka_producer)
```
或者可以用sink_to函数，可以用在流或者批模式
```python
from pyflink.datastream.connectors import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder

output_path = '/opt/output/'
file_sink = FileSink \
    .for_row_format(output_path, Encoder.simple_string_encoder()) \
    .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
    .build()
ds.sink_to(file_sink)
```
4、table & sql sink connector  
```python
from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(stream_execution_environment=env)

# option 1：the result type of ds is Types.ROW
def split(s):
    splits = s[1].split("|")
    for sp in splits:
        yield Row(s[0], sp)

ds = ds.map(lambda i: (i[0] + 1, i[1])) \
       .flat_map(split, Types.ROW([Types.INT(), Types.STRING()])) \
       .key_by(lambda i: i[1]) \
       .reduce(lambda i, j: Row(i[0] + j[0], i[1]))

# option 1：the result type of ds is Types.TUPLE
def split(s):
    splits = s[1].split("|")
    for sp in splits:
        yield s[0], sp

ds = ds.map(lambda i: (i[0] + 1, i[1])) \
       .flat_map(split, Types.TUPLE([Types.INT(), Types.STRING()])) \
       .key_by(lambda i: i[1]) \
       .reduce(lambda i, j: (i[0] + j[0], i[1]))

# emit ds to print sink
t_env.execute_sql("""
        CREATE TABLE my_sink (
          a INT,
          b VARCHAR
        ) WITH (
          'connector' = 'print'
        )
    """)

table = t_env.from_data_stream(ds)
table_result = table.execute_insert("my_sink")
```

## submit job

如果是流环境，用StreamExecutionEnvironment.execute方法： env.execute()  

如果是table环境，用TableEnvironment.execute方法： t_env.execute()