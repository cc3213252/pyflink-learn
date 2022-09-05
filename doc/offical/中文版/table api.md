临时表只在单个flink有效，存在内存中  
永久表跨flink可见

你可以先用 CEP 从 DataStream 中做模式匹配，然后用 Table API 来分析匹配的结果；
或者你可以用 SQL 来扫描、过滤、聚合一个批式的表，然后再跑一个 Gelly 图算法 来处理已经预处理好的数据

解决数据倾斜，用local-global聚合  