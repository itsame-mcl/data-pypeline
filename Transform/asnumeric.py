from Pipeline import OnVars
from copy import deepcopy


class AsNumeric(OnVars):
    """
    Transform strings to numeric values.

    ...

    Methods
    -------
    apply(df) : DataFrame
        Transforms each specified variable of the DataFrame df in a numeric form
        Raises Exception if any value can't be transformed into an int or a float

    __num(val) : int or float
        Returns val as int if possible, or as float
        Raises ValueError if val can't be converted into int or float
    """
    def apply(self, df):
        result = deepcopy(df)
        for var in self.vars:
            result[var] = [self.__num(val) for val in df[var]]
        return result

    @staticmethod
    def __num(val):
        if not (isinstance(val, int)) and not (isinstance(val, float)):
            try:
                return int(val)
            except ValueError:
                try:
                    return float(val)
                except Exception as e:
                    raise e
            except Exception as e:
                raise e
        else:
            return val
