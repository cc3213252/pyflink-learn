# encoding: utf-8
# Date: 2022/7/9 14:58

__author__ = 'yudan.chen'

from pyflink.common import Row
from pyflink.table import ListView
from pyflink.table.types import DataTypes
from pyflink.table.udf import TableAggregateFunction


class ListViewConcatTableAggregateFunction(TableAggregateFunction):

    def emit_value(self, accumulator):
        result = accumulator[1].join(accumulator[0])
        yield Row(result)
        yield Row(result)

    def create_accumulator(self):
        return Row(ListView(), '')

    def accumulate(self, accumulator, *args):
        accumulator[1] = args[1]
        accumulator[0].add(args[0])

    def get_accumulator_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("f0", DataTypes.LIST_VIEW(DataTypes.STRING())),
            DataTypes.FIELD("f1", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW([DataTypes.FIELD("a", DataTypes.STRING())])