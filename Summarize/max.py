from Summarize import SummarizeOnGroups


class Max(SummarizeOnGroups):
    """
        Calculates the maximum of a column (of a DataFrame).

        ...

        Methods
        -------
        _operation(col) : dict
            Calculates the number maximum on the col object, assuming this DataFrame represents a single group.
            The output variable is called "Variable_Max" (if col = Variable).
    """
    def _operation(self, col):
        val_max = None
        for val in col:
            if (val_max is None) or (val > val_max):
                val_max = val
        return {"Max": val_max}
