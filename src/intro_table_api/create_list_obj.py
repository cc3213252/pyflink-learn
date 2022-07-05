# encoding: utf-8
# Date: 2022/7/4 14:34

__author__ = 'yudan.chen'

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import DataTypes

# create a batch TableEnvironment
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')])
table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])

print('By default the type of the "id" column is %s.' % table.get_schema().get_field_data_type("id"))

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')],
                                DataTypes.ROW([DataTypes.FIELD("id", DataTypes.TINYINT()),
                                               DataTypes.FIELD("data", DataTypes.STRING())]))

print('Now the type of the "id" column is %s.' % table.get_schema().get_field_data_type("id"))
table.execute().print()