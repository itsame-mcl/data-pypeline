import csv
import json
from DataModel.dataframe import DataFrame


class Import:
    @classmethod
    def import_csv(cls, path, headers=True, delimiter=";", encoding='ISO-8859-1'):
        df = DataFrame()
        with open(path, encoding=encoding) as csv_file:
            reader = csv.reader(csv_file, delimiter=delimiter)
            first_row = True
            for row in reader:
                if first_row:
                    if headers:
                        for var in row:
                            df.add_column(var)
                    else:
                        for i in range(len(row)):
                            name = "Var" + str(i)
                            df.add_column(name)
                    first_row = False
                else:
                    df.add_row(row)
        return df

    @classmethod
    def import_json(cls, path, root=None):
        with open(path) as jsonfile:
            data = json.load(jsonfile)
            roots = list(data.keys())
            if len(roots) == 1 or root is None:
                root = roots[0]
            elif root not in roots:
                raise KeyError
        table = data[root]
        table_vars = list(table[0].keys())
        df = DataFrame()
        for var in table_vars:
            df.add_column(var)
        for row in table:
            df.add_row(list(row.values()))
        return df
