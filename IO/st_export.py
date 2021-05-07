from DataModel import DataFrame
import csv


class Export:
    @staticmethod
    def export_csv(df, path, headers=True, delimiter=";", encoding='ISO-8859-1'):
        if isinstance(df, DataFrame):
            with open(path, 'w', newline='', encoding=encoding) as csv_file:
                writer = csv.writer(csv_file, delimiter=delimiter)
                if headers:
                    writer.writerow(df.vars)
                writer.writerows(df)
        else:
            raise TypeError
