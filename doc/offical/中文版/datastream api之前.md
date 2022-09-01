POJO类：
1、该类是公有且独立的（没有非静态内部类）  
2、该类有公有的无参构造函数  


使用 RocksDBStateBackend 的情况下， MapState 和 ListState 比 ValueState 性能更好

状态可以设置TTL，有多种清理状态的方式  
python Datastream api不支持广播状态  

[广播流处理规则例子](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/fault-tolerance/broadcast_state/)

Evictor，用来在trigger触发后，在窗口函数的前后删除数据  

Consecutive window，可以把window分两个阶段，先5秒滚动窗口，再windowAll算topN  
异步io主要是用来异步访问数据库用的，对于不支持异步访问的数据库，可以建一个线程池模拟  

## 数据源核心组件

Split，对一部分数据包装  
SourceReader，读取分片文件  
SplitEnumerator，生成分片分配给SourceReader  
数据源是flink提供的api，用来开发实现自己的数据源  

java中使用lambda时，遇到范型类型，需要显示提供类型信息  
```java
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;

DataSet<Integer> input = env.fromElements(1, 2, 3);

// 必须声明 collector 类型
input.flatMap((Integer number, Collector<String> out) -> {
    StringBuilder builder = new StringBuilder();
    for(int i = 0; i < number; i++) {
        builder.append("a");
        out.collect(builder.toString());
    }
})
// 显式提供类型信息
.returns(Types.STRING)
// 打印 "a", "a", "aa", "a", "aa", "aaa"
.print();
```
[其他替代方法](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/java_lambdas/)