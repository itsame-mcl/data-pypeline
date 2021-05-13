from Summarize import SummarizeOnGroups


class Count(SummarizeOnGroups):
    """
        Calculates the number of observations of a column (of a DataFrame).

        ...

        Methods
        -------
        _operation(col) : dict
            Calculates the number of observations on the col object, assuming this DataFrame represents a single group.
            The number of observations is given by the length of the input.
            The output variable is called "Variable_Count" (if col = Variable).
    """
    def _operation(self, col):
        return {"Count": len(col)}
