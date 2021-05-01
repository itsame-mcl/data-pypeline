from Pipeline import Pipelineable
from Transform import Select, Filter
from DataModel import DataFrame
from copy import deepcopy


class Join(Pipelineable):
    def __init__(self, other, **matches):
        """
        Define the left join transformation

        Parameters
        ----------
        other : DataFrame
            Right hand DataFrame for the juncture
        matches : kwargs
            Pairs of matching variables, on the form : RightHand="LeftHand"
        """
        if any(not isinstance(match, str) for match in list(matches.values())):
            raise TypeError
        self.__other = other
        self.__matches = matches

    def apply(self, df):
        result = DataFrame()
        other_vars = [var for var in self.__other.vars if var not in list(self.__matches.keys())]
        for var in df.vars:
            result.add_column(var)
        for var in other_vars:
                if var in df.vars:
                    result.add_column("Y_" + str(var))
                else:
                    result.add_column(var)
        for i in range(len(df)):
            base_row = df[None, i]
            filter_kw = {}
            for key in list(self.__matches.keys()):
                target_value = df[self.__matches[key], i]
                filter_kw[key] = '=="' + str(target_value) + '"'
            matches = Filter(**filter_kw).apply(self.__other)
            if len(matches) == 0:
                nones = [None] * len(other_vars)
                base_row.extend(nones)
                result.add_row(base_row)
            else:
                matches = Select(*other_vars).apply(matches)
                for row in matches:
                    new_row = deepcopy(base_row)
                    new_row.extend(row)
                    result.add_row(new_row)
        return result
