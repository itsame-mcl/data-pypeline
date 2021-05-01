from Pipeline import Pipelineable
from copy import deepcopy


class Rename(Pipelineable):
    def __init__(self, **names):
        if any(not isinstance(name, str) for name in list(names.values())):
            raise TypeError
        self.__names = names

    def apply(self, df):
        result = deepcopy(df)
        for new_name in list(self.__names.keys()):
            result.rename_column(self.__names[new_name], new_name)
        return result
