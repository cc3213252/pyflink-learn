catalogs提供了元数据，包括databases、tables、partitions、views、functions  

数据处理关键一个方面是管理元数据，有临时的元数据，也有永久的存在hive metastore的元数据，
catalogs有管理元数据的api  

## 使用catalog或db

t_env.use_catalog("myCatalog")  
t_env.use_database("myDb")  

## 查可用catalogs

t_env.list_catalogs()  
t_env.list_databases()  
t_env.list_tables()  