# encoding: utf-8
# Date: 2022/7/12 17:24

__author__ = 'yudan.chen'

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
ds = env.from_collection(
    collection=[(1, 'aaa|bb'), (2, 'bb|a'), (3, 'aaa|a')],
    type_info=Types.ROW([Types.INT(), Types.STRING()]))