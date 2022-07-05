所有示例都在intro_table_api目录

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
+I 插入  -U retract record，消息应该被删除或撤销  +U 更新记录

凡是table的打印都应该使用[Table.limit](https://nightlies.apache.org/flink/flink-docs-release-1.15/api/python//pyflink.table.html#pyflink.table.Table.limit)

connector为blackhole的结果表，用来调试，如果您在注册其他类型的Connector结果表时报错，但您不确定是系统问题还是结果表WITH参数错误，
您可以将WITH参数修改为'connector' = 'blackhole'后，单击运行。如果不再报错，则证明系统没有问题，您需要确认修改WITH参数