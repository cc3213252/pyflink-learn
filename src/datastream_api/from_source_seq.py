# encoding: utf-8
# Date: 2022/7/12 17:25

__author__ = 'yudan.chen'

from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import NumberSequenceSource

env = StreamExecutionEnvironment.get_execution_environment()
seq_num_source = NumberSequenceSource(1, 1000)
ds = env.from_source(
    source=seq_num_source,
    watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    source_name='seq_num_source',
    type_info=Types.LONG())