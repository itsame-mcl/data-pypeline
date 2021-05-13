from Transform import TransformOnGroups
from copy import deepcopy


class Mutate(TransformOnGroups):
    """
    Create new variables in a DataFrame by taking into account the group structure.

    ...

    Attributes
    ----------
    self.__expressions : dict
        Dict of expressions, with keys as new variables names and values as formulas to compute.

    Methods
    -------
    __init__(**expressions):
        Create a Mutate object with expressions as key/values pairs. On each pair, the key is the name of the
        new variable and the value is a string of the exact Python syntax of the formula to create the variable,
        such as "Var+2" or, "(Var-lag_Var)/lag_Var". Lagged and leaded values are available with prefixes "lag_"
        and "_lead". The row index value is also available with the variable name "row_index".

    _operation(df): DataFrame
        Apply to a group DataFrame, and returns a new DataFrame, with the new variables added and computed.
    """
    def __init__(self, **expressions):
        if any(not isinstance(expression, str) for expression in list(expressions.values())):
            raise TypeError
        self.__expressions = expressions

    def _operation(self, df):
        result = deepcopy(df)
        new_vars = {}
        for new_var in list(self.__expressions.keys()):
            if new_var in result.vars:
                raise KeyError
            else:
                new_vars[new_var] = []
        for i in range(len(result)):
            row_dict = result.row_as_dict(i)
            row_dict['row_index'] = i
            for new_var in list(new_vars.keys()):
                new_val = None
                try:
                    new_val = eval(self.__expressions[new_var], {"__builtins__": {}}, row_dict)
                except TypeError:
                    pass
                except Exception as e:
                    raise e
                finally:
                    new_vars[new_var].append(new_val)
        for new_var in list(new_vars.keys()):
            result.add_column(new_var, new_vars[new_var])
        return result
