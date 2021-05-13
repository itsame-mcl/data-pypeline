from abc import ABC
from Pipeline import OnGroups
from DataModel import DataFrame
from Transform import GroupBy


class TransformOnGroups(OnGroups, ABC):
    """
    Splits data according to groups (if specified) then paste results together in the form of a DataFrame.

    ...

    Methods
    -------
    apply(df) : DataFrame
        The apply method manages the splitting of the data according to the group
        provided by the user. If no group is specified, we assume that the input data
        represents a single group. Data are transmitted to the _operation method which
        carries out the calculations needed. Finally, the apply method reassembles the results
        before returning them in the form of a single DataFrame.
    """
    def apply(self, df):
        result = DataFrame()
        groups = df.groups_df
        for group_df in groups:
            transformed_group = self._operation(group_df)
            if len(transformed_group) > 0:
                if len(result.vars) == 0:
                    for var in transformed_group.vars:
                        result.add_column(var)
                for row in transformed_group:
                    result.add_row(row)
        result = GroupBy(*df.groups_vars).apply(result)
        return result
