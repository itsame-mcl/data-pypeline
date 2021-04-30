from Pipeline.onvars import OnVars
from Transform.mutate import Mutate
from Transform.groupby import GroupBy
from DataModel import DataFrame


class Filter(OnVars):
    def __init__(self, fun_criteria, *on_vars):
        super().__init__(*on_vars)
        self.__criteria = fun_criteria

    def apply(self, df):
        result = DataFrame()
        for var in df.vars:
            result.add_column(var)
        keep = Mutate("Keep", self.__criteria, *self.vars).apply(df)["Keep"]
        for row, i in zip(df, range(len(df))):
            if keep[i]:
                result.add_row(row)
        result = GroupBy(*df.groups_vars).apply(result)
        return result
