from Pipeline import OnVars
from copy import deepcopy


class GroupBy(OnVars):
    """
    Add variables to the group structure of a DataFrame.

    ...

    Methods
    -------
    apply(df) : DataFrame
        Add the variables of the GroupBy object to the DataFrame df, and returns the new DataFrame
    """
    def apply(self, df):
        result = deepcopy(df)
        for var in self.vars:
            if var in df.vars:
                result.add_group(var)
            else:
                raise KeyError
        return result
