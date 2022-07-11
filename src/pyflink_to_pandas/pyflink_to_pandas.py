# encoding: utf-8
# Date: 2022/7/11 10:22

__author__ = 'yudan.chen'

from pyflink.table.expressions import col
from pyflink.table import EnvironmentSettings, TableEnvironment
import pandas as pd
import numpy as np

# Create a PyFlink Table
pdf = pd.DataFrame(np.random.rand(1000, 2))
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)
table = t_env.from_pandas(pdf, ["a", "b"]).filter(col('a') > 0.5)

# Convert the PyFlink Table to a Pandas DataFrame
pdf = table.limit(100).to_pandas()