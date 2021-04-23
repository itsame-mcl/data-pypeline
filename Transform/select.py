from Pipeline.onvars import OnVars
from DataModel.dataframe import DataFrame


class Select(OnVars):
    def apply(self, df):
        result = DataFrame()
        for var in self.vars:
            result.add_column(var, df[var])
        for group in df.groups:
            if group in self.vars:
                result.add_group(group)
        return result
