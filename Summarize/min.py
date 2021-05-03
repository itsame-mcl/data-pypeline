from Summarize import SummarizeOnGroups


class Min(SummarizeOnGroups):
    def _operation(self, col):
        val_min = None
        for val in col:
            if (val_min is None) or (val < val_min):
                val_min = val
        return {"Min": val_min}
