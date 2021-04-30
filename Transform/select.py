from Pipeline import OnVars
from DataModel import DataFrame
from Transform import GroupBy


class Select(OnVars):
    def apply(self, df):
        result = DataFrame()
        for var in self.vars:
            result.add_column(var, df[var])
        kept_group_vars = [var for var in df.groups_vars if var in self.vars]
        result = GroupBy(*kept_group_vars).apply(result)
        return result
