from numbers import Number
from Pipeline import OnVars
from Transform import TransformOnGroups
from Summarize import Average, Variance
from copy import deepcopy


class Normalize(OnVars, TransformOnGroups):
    def __init__(self, *on_vars, center=True, reduce=True):
        super().__init__(*on_vars)
        self.__center = center
        self.__reduce = reduce

    def _operation(self, group_df):
        result = deepcopy(group_df)
        average_df = Average(*self.vars).apply(group_df)
        sd_df = Variance(*self.vars, get_var=False).apply(group_df)
        for var in self.vars:
            new_var = group_df[var]
            if self.__center:
                average = average_df[var + "_Average", 0]
                new_var = [(x - average) if isinstance(x, Number) else None for x in new_var]
            if self.__reduce:
                sd = sd_df[var + "_SD", 0]
                new_var = [(x / sd) if isinstance(x, Number) else None for x in new_var]
            result.add_column(var + "_Std", new_var, after=var)
        return result
