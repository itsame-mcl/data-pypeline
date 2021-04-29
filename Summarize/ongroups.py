import numbers
from abc import ABC, abstractmethod
from copy import deepcopy
from Pipeline.onvars import OnVars
from DataModel.dataframe import DataFrame
from Transform.select import Select
from Transform.groupby import GroupBy


class OnGroups(OnVars, ABC):
    def __init__(self, *on_vars, ignore_na=True, ignore_nan=True):
        super().__init__(*on_vars)
        self.__na = ignore_na
        self.__nan = ignore_nan

    @abstractmethod
    def _operation(self, col):
        raise NotImplementedError

    def apply(self, df):
        list_vars = [*df.groups_vars, *self.vars]
        df = Select(*list_vars).apply(df)
        empty_df = DataFrame()
        for var in list_vars:
            empty_df.add_column(var)
        result = deepcopy(empty_df)
        groups = []
        max_group = -1
        for row, group in zip(df, df.groups):
            if group > max_group:
                groups.append(deepcopy(empty_df))
                max_group = group
            if group == 0:
                groups[0].add_row(row)
            else:
                groups[group-1].add_row(row)
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
        result = GroupBy(*df.groups_vars[:-1]).apply(result)
        return result
