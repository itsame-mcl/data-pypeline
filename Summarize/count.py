from Summarize.crawler import Crawler


class Count(Crawler):
    def apply(self, df):
        result = self._crawl(df, "1")
        return result
