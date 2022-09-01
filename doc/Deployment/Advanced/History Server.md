## 命令

bin/historyserver.sh (start|start-foreground|stop)  
端口号：8082  

## 要配置两个参数

完成job的上传目录：  
jobmanager.archive.fs.dir: hdfs:///completed-jobs  

监控目录看有没新的job（可以逗号分隔）：  
historyserver.archive.fs.dir: hdfs:///completed-jobs  

监控时间间隔：  
historyserver.archive.fs.refresh-interval: 10000

## 访问

访问所有job:  
http://hostname:8082/jobs  

访问某个job异常：  
http://hostname:port/jobs/<jobid>/exceptions