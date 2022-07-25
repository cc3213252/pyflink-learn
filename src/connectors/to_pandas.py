# encoding: utf-8
# Date: 2022/7/12 17:03

__author__ = 'yudan.chen'

from pyflink.table.expressions import col

import pandas as pd
import numpy as np

# Create a PyFlink Table
pdf = pd.DataFrame(np.random.rand(1000, 2))
table = t_env.from_pandas(pdf, ["a", "b"]).filter(col('a') > 0.5)

# Convert the PyFlink Table to a Pandas DataFrame
pdf = table.to_pandas()