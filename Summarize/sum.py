import numbers
from Pipeline.onvars import OnVars
from DataModel.dataframe import DataFrame
from Transform.select import Select


class Sum(OnVars):
    def __init__(self, arg_vars, ignore_na=True, ignore_nan=True):
        super().__init__(arg_vars)
        self.__na = ignore_na
        self.__nan = ignore_nan

    def apply(self, df):
        list_vars = [*df.groups_vars, *self.vars]
        df = Select(list_vars).apply(df)
        result = DataFrame()
        if not df.groups_vars:
            for var in list_vars:
                partial_sum = None
                for val in df[var]:
                    if val is None:
                        if self.__na:
                            continue
                        else:
                            raise ValueError
                    elif not isinstance(val, numbers.Number):
                        if self.__nan:
                            continue
                        else:
                            raise TypeError
                    else:
                        if partial_sum is None:
                            partial_sum = val
                        else:
                            partial_sum += val
                result.add_column(var, [partial_sum])
        else:
            for var in list_vars:
                result.add_column(var)
            max_group = 0
            for row, group in zip(df, df.groups):
                for i in range(len(df.groups_vars), len(list_vars)):
                    if row[i] is None:
                        if self.__na:
                            continue
                        else:
                            raise ValueError
                    elif not isinstance(row[i], numbers.Number):
                        if self.__nan:
                            row[i] = None
                        else:
                            raise TypeError
                if group > max_group:
                    result.add_row(row)
                    max_group = group
                else:
                    for i in range(len(df.groups_vars), len(list_vars)):
                        if row[i] is None:
                            row[i] = result[i, group-1]
                        else:
                            row[i] = (result[i, group-1] if (result[i, group-1] is not None) else 0) + row[i]
                    result[None, group-1] = row
        return result
