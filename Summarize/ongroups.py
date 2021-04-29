from abc import ABC, abstractmethod
from copy import deepcopy
from Pipeline.onvars import OnVars
from DataModel.dataframe import DataFrame
from Transform.select import Select
from Transform.groupby import GroupBy


class OnGroups(OnVars, ABC):
    def __init__(self, arg_vars, ignore_na=True, ignore_nan=True):
        super().__init__(arg_vars)
        self._na = ignore_na
        self._nan = ignore_nan

    @abstractmethod
    def _operation(self, col):
        raise NotImplementedError

    def apply(self, df):
        list_vars = [*df.groups_vars, *self.vars]
        df = Select(list_vars).apply(df)
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
                row.append(self._operation(group_df[var]))
            result.add_row(row)
        result = GroupBy(df.groups_vars[:-1]).apply(result)
        return result
