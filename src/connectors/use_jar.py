# encoding: utf-8
# Date: 2022/7/12 17:02

__author__ = 'yudan.chen'

from pyflink.table import TableEnvironment, EnvironmentSettings


def log_processing():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    # specify connector and format jars
    t_env.get_config().set("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/json.jar")

    source_ddl = """
            CREATE TABLE source_table(
                a VARCHAR,
                b INT
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'source_topic',
              'properties.bootstrap.servers' = 'kafka:9092',
              'properties.group.id' = 'test_3',
              'scan.startup.mode' = 'latest-offset',
              'format' = 'json'
            )
            """

    sink_ddl = """
            CREATE TABLE sink_table(
                a VARCHAR
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'sink_topic',
              'properties.bootstrap.servers' = 'kafka:9092',
              'format' = 'json'
            )
            """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    t_env.sql_query("SELECT a FROM source_table") \
        .execute_insert("sink_table").wait()


if __name__ == '__main__':
    log_processing()