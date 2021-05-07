from Pipeline import Pipelineable
from DataModel import DataFrame
import csv


class ExportCSV(Pipelineable):
    def __init__(self, path, headers=True, delimiter=";", encoding='ISO-8859-1'):
        self.__path = path
        self.__headers = headers
        self.__delimiter = delimiter
        self.__encoding = encoding

    def apply(self, df):
        if isinstance(df, DataFrame):
            with open(self.__path, 'w', newline='', encoding=self.__encoding) as csv_file:
                writer = csv.writer(csv_file, delimiter=self.__delimiter)
                if self.__headers:
                    writer.writerow(df.vars)
                writer.writerows(df)
        else:
            raise TypeError
        return df
