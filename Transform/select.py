from Pipeline import OnVars
from DataModel import DataFrame
from Transform import GroupBy


class Select(OnVars):
    """
    Select some variables from a DataFrame

    ...

    Methods
    -------
    apply(df) : DataFrame
        Create and return a new DataFrame containing only the selected variables of df, ordered in the
        order they were added on the Select object
    """
    def apply(self, df):
        result = DataFrame()
        for var in self.vars:
            result.add_column(var, df[var])
        kept_group_vars = [var for var in df.groups_vars if var in self.vars]
        result = GroupBy(*kept_group_vars).apply(result)
        return result
