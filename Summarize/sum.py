from Summarize.rowsums import RowSums


class Sum(RowSums):
    def apply(self, df):
        result = self._crawl(df, "val")
        return result
