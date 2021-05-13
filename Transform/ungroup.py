from Pipeline import Pipelineable
from copy import deepcopy


class Ungroup(Pipelineable):
    """
    Removes the group structure of a DataFrame.

    ...

    Methods
    -------
    apply(df) : DataFrame
        Returns a copy of the df DataFrame without any group structure
    """
    def apply(self, df):
        result = deepcopy(df)
        for var in df.groups_vars:
            result.del_group(var)
        return result
