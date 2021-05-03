from Summarize import SummarizeOnGroups


class Average(SummarizeOnGroups):
    def _operation(self, col):
        partial_average = None
        for val in col:
            if partial_average is None:
                partial_average = val / len(col)
            else:
                partial_average += val / len(col)
        return {"Average": partial_average}
