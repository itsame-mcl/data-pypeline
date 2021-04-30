import numbers
import Transform
from abc import ABC
from Pipeline import OnVars, OnGroups
from DataModel import DataFrame


class SummarizeOnGroups(OnVars, OnGroups, ABC):
    def __init__(self, *on_vars, ignore_na=True, ignore_nan=True):
        super().__init__(*on_vars)
        self.__na = ignore_na
        self.__nan = ignore_nan

    def apply(self, df):
        list_vars = [*df.groups_vars, *self.vars]
        df = Transform.Select(*list_vars).apply(df)
        result = DataFrame()
        for var in list_vars:
            result.add_column(var)
        groups = df.groups_df
        for group_df in groups:
            row = []
            for group_var in df.groups_vars:
                row.append(group_df[group_var, 0])
            for var in self.vars:
                col = group_df[var]
                if any(val is None for val in col):
                    if self.__na:
                        col = [val for val in col if val is not None]
                    else:
                        raise ValueError
                if any(not isinstance(val, numbers.Number) for val in col):
                    if self.__nan:
                        col = [val for val in col if isinstance(val, numbers.Number)]
                    else:
                        raise TypeError
                partial_result = self._operation(col)
                if isinstance(partial_result, dict):
                    keys = list(partial_result.keys())
                    if (var + "_" + keys[0]) not in result.vars:
                        last = var
                        for key in keys:
                            new_var = var + "_" + key
                            result.add_column(new_var, after=last)
                            last = new_var
                        result.del_column(var)
                    row.extend(list(partial_result.values()))
                else:
                    row.append(partial_result)
            result.add_row(row)
        result = Transform.GroupBy(*df.groups_vars[:-1]).apply(result)
        return result
