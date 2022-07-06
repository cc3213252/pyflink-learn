# encoding: utf-8
# Date: 2022/7/6 16:30

__author__ = 'yudan.chen'

import pandas as pd
from pyflink.table.udf import udf
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.types import DataTypes


env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])


@udf(result_type=DataTypes.ROW([DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("data", DataTypes.STRING())]),
     func_type='pandas')
def func3(data: pd.DataFrame) -> pd.DataFrame:
    res = pd.concat([data.id, data.data * 2], axis=1)
    return res


table.map(func3).execute().print()
