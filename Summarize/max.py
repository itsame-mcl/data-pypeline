from Summarize import SummarizeOnGroups


class Max(SummarizeOnGroups):
    def _operation(self, col):
        val_max = None
        for val in col:
            if (val_max is None) or (val > val_max):
                val_max = val
        return {"Max": val_max}
