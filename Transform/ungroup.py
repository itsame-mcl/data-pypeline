from Pipeline import Pipelineable
from copy import deepcopy


class Ungroup(Pipelineable):
    def apply(self, df):
        result = deepcopy(df)
        for var in df.groups_vars:
            result.del_group(var)
        return result
