from Summarize.crawler import Crawler


class Sum(Crawler):
    def apply(self, df):
        result = self._crawl(df, "val")
        return result