from Summarize.rowsums import RowSums


class Count(RowSums):
    def apply(self, df):
        result = self._crawl(df, "1")
        return result
