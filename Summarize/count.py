import numbers
from Summarize.ongroups import OnGroups


class Count(OnGroups):
    def _operation(self, col):
        partial_count = None
        for val in col:
            if val is None:
                if self._na:
                    continue
                else:
                    raise ValueError
            elif not isinstance(val, numbers.Number):
                if self._nan:
                    continue
                else:
                    raise TypeError
            else:
                if partial_count is None:
                    partial_count = 1
                else:
                    partial_count += 1
        return {"Count": partial_count}
