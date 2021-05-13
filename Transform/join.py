from Pipeline import Pipelineable
from Transform import Select, Filter
from DataModel import DataFrame
from copy import deepcopy


class Join(Pipelineable):
    """
    Performs a left join transformation between two DataFrames.

    The right join should be performed by inverting left hand and right hand, the full join by performing
    both left and right join and computing the union between the results and the inner join by performing
    both left and right joint and computing the intersection between the results.

    ...

    Methods
    -------

    __init__(other, **matches)
        Create the Join object, with other as right hand DataFrame, and the matching criteria as key/values pairs.
        The key is the name of a variable in the right hand, and the value is the name of the matching variable
        in the left hand as string.

    apply(df): DataFrame
        Execute the left join operation with DataFrame df as left hand. Returns a new DataFrame, with all left
        hand variables and the non matching variables of the right hand DataFrame. If a variable exists in both,
        the right hand one will be renamed as "Y_Variable".
    """
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
        known_matches = {}
        for i in range(len(df)):
            base_row = df[None, i]
            filter_kw = {}
            filter_str = ""
            for key in list(self.__matches.keys()):
                target_value = df[self.__matches[key], i]
                filter_kw[key] = '=="' + str(target_value) + '"'
                filter_str += str(key) + "_" + str(target_value)
            if known_matches.get(filter_str) is None:
                matches = Filter(**filter_kw).apply(self.__other)
                if len(matches) == 0:
                    other_content = [None] * len(other_vars)
                else:
                    other_content = Select(*other_vars).apply(matches)
                known_matches[filter_str] = other_content
            else:
                other_content = known_matches[filter_str]
            if isinstance(other_content, DataFrame):
                for row in other_content:
                    new_row = deepcopy(base_row)
                    new_row.extend(row)
                    result.add_row(new_row)
            else:
                new_row = deepcopy(base_row)
                new_row.extend(other_content)
                result.add_row(new_row)
        return result
