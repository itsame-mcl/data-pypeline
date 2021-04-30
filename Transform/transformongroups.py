import Transform
from abc import ABC
from Pipeline import OnGroups
from DataModel import DataFrame


class TransformOnGroups(OnGroups, ABC):
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
        result = Transform.GroupBy(*df.groups_vars).apply(result)
        return result
