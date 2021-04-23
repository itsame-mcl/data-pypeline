from Pipeline.onvars import OnVars
from DataModel.dataframe import DataFrame


class GroupBy(OnVars):
    def apply(self, df):
        result = DataFrame(df.dict)
        for var in self.vars:
            if var in df.vars:
                result.add_group(var)
            else:
                raise KeyError
        return result
