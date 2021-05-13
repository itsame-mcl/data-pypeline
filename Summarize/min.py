from Summarize import SummarizeOnGroups


class Min(SummarizeOnGroups):
    """
        Calculates the minimum of a column (of a DataFrame).

        ...

        Methods
        -------
        _operation(col) : dict
            Calculates the number minimum on the col object, assuming this DataFrame represents a single group.
            The output variable is called "Variable_Min" (if col = Variable).
    """
    def _operation(self, col):
        val_min = None
        for val in col:
            if (val_min is None) or (val < val_min):
                val_min = val
        return {"Min": val_min}
