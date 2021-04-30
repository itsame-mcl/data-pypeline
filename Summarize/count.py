from Summarize import SummarizeOnGroups


class Count(SummarizeOnGroups):
    def _operation(self, col):
        return {"Count": len(col)}
