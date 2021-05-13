from numbers import Number
from Pipeline import OnVars
from Transform import TransformOnGroups, Sort


class MovingAverage(OnVars, TransformOnGroups):
    """
    Compute a moving average for a list of variables on a DataFrame, by taking into account it's group structure.

    ...

    Attributes
    ----------
    self.__window : int
        Window size for the moving average, as a number of lines
    self.__time_var : str
        Name of the variable giving the time information, to sort the DataFrame

    Methods
    -------
    __init__(window, time_var, *on_vars)
        Setup the MovingAverage computation by defining window size, the time variable (time_var) and the
        variables for which the moving average should be calculated.

    _operation(group_df): DataFrame
        Performs the computation on each group_df DataFrame, by sorting them with respect of the time_var
        variable, and inserts columns named "Variable_MA<Window>" with the results, for example "Val_MA5" for
        a moving average of window 5 on the variable Val. When a value can't be computed by lack of data, a
        None value is inserted.
    """
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
