import numbers
from Pipeline.onvars import OnVars
from DataModel.dataframe import DataFrame
from Transform.select import Select
from Transform.groupby import GroupBy


class RowSums(OnVars):
    def __init__(self, arg_vars, ignore_na=True, ignore_nan=True):
        super().__init__(arg_vars)
        self.__na = ignore_na
        self.__nan = ignore_nan

    def _crawl(self, df, expr):
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
                            partial_sum = eval(expr)
                        else:
                            partial_sum += eval(expr)
                result.add_column(var, [partial_sum])
        else:
            for var in list_vars:
                result.add_column(var)
            max_group = 0
            for row, group in zip(df, df.groups):
                for val, i in zip(row[len(df.groups_vars):], range(len(df.groups_vars), len(list_vars))):
                    if val is None:
                        if self.__na:
                            continue
                        else:
                            raise ValueError
                    elif not isinstance(val, numbers.Number):
                        if self.__nan:
                            row[i] = None
                        else:
                            raise TypeError
                    else:
                        row[i] = eval(expr)
                if group > max_group:
                    result.add_row(row)
                    max_group = group
                else:
                    for val, i in zip(row[len(df.groups_vars):], range(len(df.groups_vars), len(list_vars))):
                        if val is None:
                            row[i] = result[i, group-1]
                        else:
                            row[i] = (result[i, group-1] if (result[i, group-1] is not None) else 0) + eval(expr)
                    result[None, group-1] = row
        result = GroupBy(df.groups_vars[:-1]).apply(result)
        return result

    def apply(self, df):
        raise NotImplementedError
