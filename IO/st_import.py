from DataModel import DataFrame
import csv
import json


class Import:
    @staticmethod
    def import_csv(path, headers=True, delimiter=";", encoding='ISO-8859-1'):
        """
        Imports a CSV file as DataFrame

        Parameters
        ----------
        path : str
            Absolute or relative path to the CSV file to import
        headers : bool = True
            Specify if the file have headers
        delimiter : str = ";"
            Specify the file's delimiter
        encoding : str = 'ISO-8859-1'
            Specify the file's encoding

        Returns
        -------
        DataFrame
            A DataFrame with the contents of the CSV file
        """
        df = DataFrame()
        with open(path, newline='', encoding=encoding) as csv_file:
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

    @staticmethod
    def import_json(path, root=None):
        """
        Imports a JSON file as DataFrame.

        Parameters
        ----------
        path : str
            Absolute or relative path to the JSON file to import
        root : str = None
            Name of the root's node to import ; if None, imports the first root node of the file

        Returns
        -------
        DataFrame
            A DataFrame with the contents of the JSON file
        """
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
