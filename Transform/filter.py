from Pipeline import Pipelineable
from DataModel import DataFrame
from Transform.groupby import GroupBy


class Filter(Pipelineable):
    def __init__(self, **criteria):
        if any(not isinstance(criterion, str) for criterion in list(criteria.values())):
            raise TypeError
        self.__criteria = criteria

    def apply(self, df):
        vars_with_criterion = list(self.__criteria.keys())
        if any(var not in df.vars for var in vars_with_criterion):
            raise KeyError
        result = DataFrame()
        for var in df.vars:
            result.add_column(var)
        for i in range(len(df)):
            add_row = True
            for var in vars_with_criterion:
                test_result = eval("df['" + str(var) + "'," + str(i) + "] " + str(self.__criteria[var]),
                                   {"__builtins__": {}}, {'df': df})
                if isinstance(test_result, bool):
                    add_row *= test_result
                else:
                    raise TypeError
            if add_row:
                result.add_row(df[None, i])
        result = GroupBy(*df.groups_vars).apply(result)
        return result
