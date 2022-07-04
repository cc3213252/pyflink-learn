## 创建环境

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)

## 从文件读取数据

ds = env.from_source(
    source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                               input_path)
                     .process_static_file_set().build(),
    watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    source_name="file_source"
)

## 两个wordcount比较

wordcount1中from_elements数据一个一个送过去  
wordcount2中from_collection数据一次批量送过去  
wordcount1用table api  
wordcount2用流处理api  