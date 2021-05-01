from numbers import Number
from Pipeline import OnVars
from Transform import TransformOnGroups, Sort


class MovingAverage(OnVars, TransformOnGroups):
    def __init__(self, window, time_var, *on_vars):
        super().__init__(*on_vars)
        if isinstance(window, int):
            self.__window = window
        else:
            raise TypeError
        if isinstance(time_var, str):
            self.__time_var = time_var
        else:
            raise TypeError

    def _operation(self, group_df):
        result = Sort(self.__time_var).apply(group_df)
        for var in self.vars:
            signal = result[var]
            ma = [None] * len(signal)
            for i in range(self.__window - 1, len(signal)):
                if all(isinstance(val, Number) for val in signal[i - self.__window + 1:i + 1]):
                    partial_ma = 0.0
                    for val in signal[i - self.__window + 1:i + 1]:
                        partial_ma += val
                    partial_ma /= self.__window
                    # noinspection PyTypeChecker
                    ma[i] = partial_ma
            result.add_column(var + "_MA" + str(self.__window), ma, after=var)
        return result
