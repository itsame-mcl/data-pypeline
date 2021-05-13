from Pipeline import Pipelineable
from copy import deepcopy


class Rename(Pipelineable):
    """
    Rename variables in a DataFrame.

    ...

    Attributes
    ----------
    self.__names : dict
        Dict of renames operations, with keys as new names and values as old names

    Methods
    -------
    __init__(**names):
        Create a Rename object with new/old names as key/values pairs. On each pair, the key is the new name of
        the variable and the value is the current name of the variable as a string.

    apply(df): DataFrame
        Apply the Rename to the DataFrame df, renaming in place each variable from their old name to new name.
    """
    def __init__(self, **names):
        if any(not isinstance(name, str) for name in list(names.values())):
            raise TypeError
        self.__names = names

    def apply(self, df):
        result = deepcopy(df)
        for new_name in list(self.__names.keys()):
            result.rename_column(self.__names[new_name], new_name)
        return result
