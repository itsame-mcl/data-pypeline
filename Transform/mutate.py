from Pipeline.onvars import OnVars
from Transform.select import Select
from copy import deepcopy


class Mutate(OnVars):
    def __init__(self, arg_vars, fun, new_var):
        super().__init__(arg_vars)
        self.__fun = fun
        self.__new_var = str(new_var)

    def apply(self, df):
        result = deepcopy(df)
        inter = Select(self.vars).apply(df)
        new_var_data = []
        for row in inter:
            new_var_data.append(self.__fun(*row))
        result.add_column(self.__new_var, new_var_data)
        return result
