转换有一点开销，这个开销可以忽略不计  

建议的方式是在datastream api中配置所有参数  

table api中分支管道尽支持使用StatementSet，见table_statementset.py  

只有转换成流后，才能直接使用打印：  
table_env.to_data_stream(table).print()  

## 流处理创建env

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
table_env = StreamTableEnvironment.create(env)

或：  

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env, EnvironmentSettings.in_batch_mode())

批处理下不能使用watermark，不能用checkpointing，不能产生更新流，只有插入流  

可以使用numer-of-rows转换为有界流，或者说如何用datagen产生有界流，见number_of_rows.py  

一个双流join，根据id和时间匹配，聚合名称例子twostream_changelog.py  

## 函数集

### insert only 

fromDataStream(DataStream)  
fromDataStream(DataStream, Schema)  
createTemporaryView(String, DataStream)， createTemporaryView(String, fromDataStream(DataStream))    
createTemporaryView(String, DataStream, Schema)，createTemporaryView(String, fromDataStream(DataStream, Schema))   
toDataStream(Table)  
toDataStream(Table, AbstractDataType)  
toDataStream(Table, Class)，toDataStream(Table, DataTypes.of(Class))  

流转换表较好示例：exp_fromdatastream.py  

只有非更新table可以使用toDataStream方法  

### changelog streams

fromChangelogStream(DataStream)  
fromChangelogStream(DataStream, Schema)  
fromChangelogStream(DataStream, Schema, ChangelogMode)  insert-only, upsert, or retract  
toChangelogStream(Table)  
toChangelogStream(Table, Schema)  
toChangelogStream(Table, Schema, ChangelogMode)  

exp_fromchangelogstream.py展示了如何通过使用upsert模式将更新消息的数量减少50%来限制传入更改的种类，从而提高效率。
通过为toChangelogStream定义一个主键和upsert变更日志管理模式，可以减少结果消息的数量

implicit conversions in scala开始到最后水过  