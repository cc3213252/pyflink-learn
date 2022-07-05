## create TableEnvironment

from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

## create list object

创建简单obj:  
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')])  

指定列名创建（这种方式id是bigint）：  
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])  

如果没有自动创建表结构，可以指定创建：  
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')],
                                DataTypes.ROW([DataTypes.FIELD("id", DataTypes.TINYINT()),
                                               DataTypes.FIELD("data", DataTypes.STRING())]))  
例子create_list_obj.py

## DDL方式创建

略，用datagen，就是gdc-olap-py使用的  

## 用TableDescriptor创建

这种方式类似ddl，就是wordcount.py使用的  

# Catalog

TableEnvironment维护一个用标识符创建的表的目录映射  
create table 或create view都是存在catalog里面的  

## 通过from_path，从一个catalog中创建table

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
table_env.create_temporary_view('source_table', table)

new_table = table_env.from_path('source_table')
new_table.execute().print()

# sql查询

flink sql基于apache calcite  
[flink sql支持的操作](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/overview/)

较好的测试随机数： datagen  
Datagen源表是系统内置的Connector，可以周期性地生成Datagen源表对应类型的随机数据  
例子： sql_egg_query.py  