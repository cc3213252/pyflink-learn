## 加载资源

```python
class Predict(ScalarFunction):
    def open(self, function_context):
        import pickle

        with open("resources.zip/resources/model.pkl", "rb") as f:
            self.model = pickle.load(f)

    def eval(self, x):
        return self.model.predict(x)

predict = udf(Predict(), result_type=DataTypes.DOUBLE(), func_type="pandas")
```

## 测试udf

add = udf(lambda i, j: i + j, result_type=DataTypes.BIGINT())  
f = add._func  
assert f(1, 2) == 3  

## udf的三种返回类型

@udtf(result_types=DataTypes.BIGINT())
def generator_func(x):
      yield 1
      yield 2

@udtf(result_types=DataTypes.BIGINT())
def iterator_func(x):
      return range(5)

@udtf(result_types=DataTypes.BIGINT())
def iterable_func(x):
      result = [1, 2, 3]
      return result

## 聚合函数

较好程序udf_agg.py，有[示意图](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/python/table/udfs/python_udfs/)  
udf中的聚合函数目前只支持流模式，批模式下建议使用量化聚合函数  
聚合计算量大的话用Listview和map，如udf_agg_listview.py  

以下四个配置可以调节python udf性能：  
python.state.cache-size, 
python.map-state.read-cache-size, 
python.map-state.write-cache-size, 
python.map-state.iterate-response-batch-size

极其重要：table aggregate functions，例子：udf_table_agg.py  
retract这个方法例子没实现但是强烈建议实现  
进阶：udf_table_agg_listview.py  