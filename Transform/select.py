from Pipeline.onvars import OnVars
from DataModel.dataframe import DataFrame


class Select(OnVars):
    def __init__(self,arg_vars):
        super().__init__(arg_vars)

    def apply(self, df):
        result = DataFrame()
        for var in self.get_vars:
            result.add_column(var, df[var])
        return result
