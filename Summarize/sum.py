from Summarize import SummarizeOnGroups


class Sum(SummarizeOnGroups):
    """
        Calculates the sum of a column (of a DataFrame).

        ...

        Methods
        -------
        _operation(col) : dict
            Calculates the total of all observations on the col object,
            assuming this DataFrame represents a single group.
            The output variable is called "Variable_Sum" (if col = Variable).
    """
    def _operation(self, col):
        partial_sum = None
        for val in col:
            if partial_sum is None:
                partial_sum = val
            else:
                partial_sum += val
        return {"Sum": partial_sum}
