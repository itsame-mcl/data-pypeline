from Summarize import SummarizeOnGroups


class Sum(SummarizeOnGroups):
    def _operation(self, col):
        partial_sum = None
        for val in col:
            if partial_sum is None:
                partial_sum = val
            else:
                partial_sum += val
        return {"Sum": partial_sum}
