from Pipeline.onvars import OnVars
from copy import deepcopy


class GroupBy(OnVars):
    def apply(self, df):
        result = deepcopy(df)
        for var in self.vars:
            if var in df.vars:
                result.add_group(var)
            else:
                raise KeyError
        return result
