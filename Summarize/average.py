from Summarize import SummarizeOnGroups


class Average(SummarizeOnGroups):
    """
        Calculates the arithmetic average for a column (of a DataFrame).

        ...

        Methods
        -------
        _operation(col) : dict
            Calculates the average on the col object, assuming this DataFrame represents a single group
        """
    def _operation(self, col):
        partial_average = None
        for val in col:
            if partial_average is None:
                partial_average = val / len(col)
            else:
                partial_average += val / len(col)
        return {"Average": partial_average}
