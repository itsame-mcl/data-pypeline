from Transform import TransformOnGroups
from DataModel import DataFrame


class Filter(TransformOnGroups):
    def __init__(self, **criteria):
        if any(not isinstance(criterion, str) for criterion in list(criteria.values())):
            raise TypeError
        self.__criteria = criteria

    def _operation(self, df):
        vars_with_criterion = list(self.__criteria.keys())
        if any(var not in df.vars for var in vars_with_criterion):
            raise KeyError
        result = DataFrame()
        for var in df.vars:
            result.add_column(var)
        for i in range(len(df)):
            row_dict = df.row_as_dict(i)
            add_row = True
            for var in vars_with_criterion:
                test_result = False
                try:
                    test_result = eval(str(var) + " " + str(self.__criteria[var]), {"__builtins__": {}}, row_dict)
                except TypeError:
                    pass
                except Exception as e:
                    raise e
                finally:
                    if isinstance(test_result, bool):
                        add_row *= test_result
                    else:
                        raise TypeError
            if add_row:
                result.add_row(df[None, i])
        return result
