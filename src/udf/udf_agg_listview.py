# encoding: utf-8
# Date: 2022/7/9 14:38

__author__ = 'yudan.chen'

from pyflink.common import Row
from pyflink.table import ListView
from pyflink.table import AggregateFunction, DataTypes


class ListViewConcatAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        # the ListView is iterable
        return accumulator[1].join(accumulator[0])

    def create_accumulator(self):
        return Row(ListView(), '')

    def accumulate(self, accumulator, *args):
        accumulator[1] = args[1]
        # the ListView support add, clear and iterate operations.
        accumulator[0].add(args[0])

    def get_accumulator_type(self):
        return DataTypes.ROW([
            # declare the first column of the accumulator as a string ListView.
            DataTypes.FIELD("f0", DataTypes.LIST_VIEW(DataTypes.STRING())),
            DataTypes.FIELD("f1", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.STRING()