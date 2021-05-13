from abc import ABC
from numbers import Number
from Pipeline import OnVars, OnGroups
from DataModel import DataFrame
from Transform import Select, GroupBy


class SummarizeOnGroups(OnVars, OnGroups, ABC):
    """
    Splits data according to groups (if specified) then paste results together in the form of a DataFrame.

    ...

    Attributes
    ----------
    __del_na : bool
        indicates if we delete NAs or not, by default NA are removed
    __del_nan : bool
        indicates if we delete NaNs or not, by default NaNs are removed

    Methods
    -------
    apply(df) : DataFrame
        The apply method manages the splitting of the data according to the group
        provided by the user. If no group is specified, we assume that the input data
        represents a single group. Data are transmitted to the _operation method which
        carries out the calculations needed (average, min, max and so on).
        Finally, the apply method reassembles the results before returning them in
        the form of a DataFrame.
    """
    def __init__(self, *on_vars, delete_na=True, delete_nan=True):
        super().__init__(*on_vars)
        self.__del_na = delete_na
        self.__del_nan = delete_nan

    def apply(self, df):
        list_vars = [*df.groups_vars, *self.vars]
        df = Select(*list_vars).apply(df)
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
                if self.__del_na:
                    col = [val for val in col if val is not None]
                if self.__del_nan:
                    col = [val for val in col if isinstance(val, Number)]
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
