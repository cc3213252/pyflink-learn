## 推荐的是用EnvironmentSettings

```python
from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment

# create a streaming TableEnvironment
config = Configuration()
config.set_string('execution.buffer-timeout', '1 min')
env_settings = EnvironmentSettings \
    .new_instance() \
    .in_streaming_mode() \
    .with_configuration(config) \
    .build()

table_env = TableEnvironment.create(env_settings)
```

## 创建流处理环境

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# create a streaming TableEnvironment from a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
```

execute_sql这个方法对于insert into语句来说，是一个异步的过程，在IDE中需要加wait  

网上很多程序有register_table这个函数，这个已经被弃用了，用create_temporary_view代替

## python扩展

add_python_file  
set_python_requirements  
add_python_archive  
自定义udf需要安装包这种情况时，通过这三个函数，把依赖装到指定的worker上  

## 常用的配置

table_env.get_config().set("parallelism.default", "8")
table_env.get_config().set("pipeline.name", "my_first_job")