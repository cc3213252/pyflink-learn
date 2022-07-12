# encoding: utf-8
# Date: 2022/7/12 09:41

__author__ = 'yudan.chen'

from pyflink.table import DataTypes
from pyflink.common.typeinfo import Types

t_env = ...

table = t_env.from_elements([("john", 35), ("sarah", 32)],
              DataTypes.ROW([DataTypes.FIELD("name", DataTypes.STRING()),
                             DataTypes.FIELD("age", DataTypes.INT())]))

# Convert the Table into an append DataStream of Row by specifying the type information
ds_row = t_env.to_append_stream(table, Types.ROW([Types.STRING(), Types.INT()]))

# Convert the Table into an append DataStream of Tuple[str, int] with TypeInformation
ds_tuple = t_env.to_append_stream(table, Types.TUPLE([Types.STRING(), Types.INT()]))

# Convert the Table into a retract DataStream of Row by specifying the type information
# A retract stream of type X is a DataStream of Tuple[bool, X].
# The boolean field indicates the type of the change.
# True is INSERT, false is DELETE.
retract_stream = t_env.to_retract_stream(table, Types.ROW([Types.STRING(), Types.INT()]))