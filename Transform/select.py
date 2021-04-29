from Pipeline.onvars import OnVars
from DataModel.dataframe import DataFrame
from Transform.groupby import GroupBy


class Select(OnVars):
    def apply(self, df):
        result = DataFrame()
        for var in self.vars:
            result.add_column(var, df[var])
        result = GroupBy(*df.groups_vars).apply(result)
        return result
