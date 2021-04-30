from Pipeline import Pipelineable
from copy import deepcopy


class Mutate(Pipelineable):
    def __init__(self, **expressions):
        if any(not isinstance(expression, str) for expression in list(expressions.values())):
            raise TypeError
        self.__expressions = expressions

    def apply(self, df):
        result = deepcopy(df)
        new_vars = {}
        for new_var in list(self.__expressions.keys()):
            if new_var in result.vars:
                raise KeyError
            else:
                new_vars[new_var] = []
        for row in result:
            row_dict = {}
            for var, val in zip(result.vars, row):
                row_dict[var] = val
            for new_var in list(new_vars.keys()):
                new_val = eval(self.__expressions[new_var], {"__builtins__": {}}, row_dict)
                new_vars[new_var].append(new_val)
        for new_var in list(new_vars.keys()):
            result.add_column(new_var, new_vars[new_var])
        return result
