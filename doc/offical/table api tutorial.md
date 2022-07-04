## 创建环境

t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t_env.get_config().set("parallelism.default", "1")

## 创建table

t_env.create_temporary_table(
    'source',
    TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .build())
        .option('path', input_path)
        .format('csv')
        .build())
tab = t_env.from_path('source')

t_env.create_temporary_table(
    'sink',
    TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .column('count', DataTypes.BIGINT())
                .build())
        .option('path', output_path)
        .format(FormatDescriptor.for_format('canal-json')
                .build())
        .build())

## DDL创建table

my_source_ddl = """
    create table source (
        word STRING
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{}'
    )
""".format(input_path)

my_sink_ddl = """
    create table sink (
        word STRING,
        `count` BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'canal-json',
        'path' = '{}'
    )
""".format(output_path)

t_env.execute_sql(my_source_ddl)
t_env.execute_sql(my_sink_ddl)

## 使用udf

@udtf(result_types=[DataTypes.STRING()])
def split(line: Row):
    for s in line[0].split():
        yield Row(s)

## 根据入参执行不同程序

wordcount.py  

提交一个远程job参考： https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/cli/#submitting-pyflink-jobs