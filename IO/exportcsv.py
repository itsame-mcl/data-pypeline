from Pipeline import Pipelineable
from DataModel import DataFrame
import csv


class ExportCSV(Pipelineable):
    """
    Exports a DataFrame as CSV file.

    ...

    Attributes
    ----------
    self.__path : str
        Absolute or relative path of the CSV file to generate
    self.__headers : bool
        Indicates if the headers of the DataFrame will be exported or not
    self.__delimiter : str
        Specify the delimiter character on the CSV file
    self.__encoding : str
        Specify the encoding of the CSV file

    Methods
    -------
    __init__(path, headers=True, delimiter=";", encoding='ISO-8859-1')
        Create an ExportCSV pipelinable object and define the specifications of the future CSV file.
    apply(df)
        Takes the DataFrame df and exports it as CSV file
    """
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
