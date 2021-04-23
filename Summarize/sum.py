import numbers
from Pipeline.onvars import OnVars
from DataModel.dataframe import DataFrame
from Transform.select import Select


class Sum(OnVars):
    def __init__(self, arg_vars, ignore_na = True, ignore_nan = True):
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
                            pass
                        else:
                            raise ValueError
                    elif not isinstance(val, numbers.Number):
                        if self.__nan:
                            pass
                        else:
                            raise TypeError
                    else:
                        if partial_sum is None:
                            partial_sum = val
                        else:
                            partial_sum += val
                result.add_column(var, [partial_sum])
        return result
