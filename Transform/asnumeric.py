from Pipeline import OnVars
from copy import deepcopy


class AsNumeric(OnVars):
    def apply(self, df):
        result = deepcopy(df)
        for var in self.vars:
            result[var] = [self.__num(val) for val in df[var]]
        return result

    @staticmethod
    def __num(val):
        try:
            return int(val)
        except ValueError:
            try:
                return float(val)
            except Exception as e:
                raise e
        except Exception as e:
            raise e
