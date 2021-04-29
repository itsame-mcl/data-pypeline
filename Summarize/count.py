from Summarize.ongroups import OnGroups


class Count(OnGroups):
    def _operation(self, col):
        return {"Count": len(col)}
