from Pipeline import OnVars
from copy import deepcopy


class Round(OnVars):
    """
    Round numerical data with the desired number of commas.

    ...

    Attributes
    ----------
    self.__precision : int
        desired number of decimal places

    Methods
    -------
    apply(df) : DataFrame
        Rounds each variable of a DataFrame df to a given precision
    """
    def __init__(self, *on_vars, precision=2):
        super().__init__(*on_vars)
        self.__precision = precision

    def apply(self, df):
        result = deepcopy(df)
        for var in self.vars:
            result[var] = [float(int((val * 10 ** self.__precision))) / (10 ** self.__precision) if val is not None else None for val in df[var]]
        return result
