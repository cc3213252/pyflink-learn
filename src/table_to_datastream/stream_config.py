# encoding: utf-8
# Date: 2022/7/11 13:24

__author__ = 'yudan.chen'

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode


# create Python DataStream API
env = StreamExecutionEnvironment.get_execution_environment()

# set various configuration early
env.set_max_parallelism(256)

env.get_config().add_default_kryo_serializer("type_class_name", "serializer_class_name")

env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# then switch to Python Table API
t_env = StreamTableEnvironment.create(env)

# set configuration early
t_env.get_config().set_local_timezone("Europe/Berlin")
