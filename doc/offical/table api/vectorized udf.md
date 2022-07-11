向量化udf比非向量化序列化和非序列化性能高很多  
可以用pandas、numpy等库，这些库高度优化过的，提供了高性能的数据结构  

向量化聚合函数，目前不支持RowType and MapType这两种返回值  