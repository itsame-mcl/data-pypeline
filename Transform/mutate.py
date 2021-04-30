from Pipeline import OnVars
from Transform.select import Select
from Transform.ungroup import Ungroup
from copy import deepcopy


class Mutate(OnVars):
    def __init__(self, new_var, fun, *on_vars):
        super().__init__(*on_vars)
        self.__fun = fun
        self.__new_var = str(new_var)

    def apply(self, df):
        result = deepcopy(df)
        inter = Ungroup().apply(df)
        inter = Select(*self.vars).apply(inter)
        new_var_data = []
        for row in inter:
            new_var_data.append(self.__fun(*row))
        result.add_column(self.__new_var, new_var_data)
        return result
