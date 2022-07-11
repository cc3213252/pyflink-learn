# encoding: utf-8
# Date: 2022/7/11 10:14

__author__ = 'yudan.chen'

from pyflink.table import DataTypes
from pyflink.table.udf import AggregateFunction, udaf


# option 1: extending the base class `AggregateFunction`
class MaxAdd(AggregateFunction):

    def open(self, function_context):
        mg = function_context.get_metric_group()
        self.counter = mg.add_group("key", "value").counter("my_counter")
        self.counter_sum = 0

    def get_value(self, accumulator):
        # counter
        self.counter.inc(10)
        self.counter_sum += 10
        return accumulator[0]

    def create_accumulator(self):
        return []

    def accumulate(self, accumulator, *args):
        result = 0
        for arg in args:
            result += arg.max()
        accumulator.append(result)


max_add = udaf(MaxAdd(), result_type=DataTypes.BIGINT(), func_type="pandas")


# option 2: Python function
@udaf(result_type=DataTypes.BIGINT(), func_type="pandas")
def max_add(i, j):
    return i.max() + j.max()


# option 3: lambda function
max_add = udaf(lambda i, j: i.max() + j.max(), result_type=DataTypes.BIGINT(), func_type="pandas")


# option 4: callable function
class CallableMaxAdd(object):
    def __call__(self, i, j):
        return i.max() + j.max()


max_add = udaf(CallableMaxAdd(), result_type=DataTypes.BIGINT(), func_type="pandas")


# option 5: partial function
def partial_max_add(i, j, k):
    return i.max() + j.max() + k


max_add = udaf(functools.partial(partial_max_add, k=1), result_type=DataTypes.BIGINT(), func_type="pandas")
