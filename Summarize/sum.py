import numbers
from Summarize.ongroups import OnGroups


class Sum(OnGroups):
    def _operation(self, col):
        partial_sum = None
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
                if partial_sum is None:
                    partial_sum = val
                else:
                    partial_sum += val
        return {"Sum": partial_sum}
