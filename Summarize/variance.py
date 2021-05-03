from Summarize import SummarizeOnGroups
from math import sqrt


class Variance(SummarizeOnGroups):
    def __init__(self, *on_vars, ignore_na=True, ignore_nan=True, get_var=True, get_sd=True):
        super().__init__(*on_vars, ignore_na=ignore_na, ignore_nan=ignore_nan)
        self.__get_var = get_var
        self.__get_sd = get_sd

    def _operation(self, col):
        n = len(col)
        partial_average = None
        partial_average_squared = None
        for val in col:
            if partial_average is None and partial_average_squared is None:
                partial_average = val / n
                partial_average_squared = val**2 / n
            else:
                partial_average += val / n
                partial_average_squared += val**2 / n
        var = (partial_average_squared - partial_average**2)
        sd = sqrt(var)
        res = dict()
        if self.__get_var:
            res["Var"] = var
        if self.__get_sd:
            res["SD"] = sd
        return res
