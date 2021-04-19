class DataFrame:

    def __init__(self, data=None):
        if data is None:
            self.__columns = []
            self.__data = {}
        elif isinstance(data, dict):
            self.__columns = list(data.keys())
            self.__data = data
        else:
            raise TypeError

    def __getitem__(self, item):
        if isinstance(item, str):
            if item in self.__columns:
                return self.__data[item]
        elif isinstance(item, int):
            return self.__data[self.__columns[item]]
        elif isinstance(item, tuple):
            if len(item) == 1:
                return self.__data[self.__columns[item[0]]]
            elif len(item) == 2:
                if (item[0] is not None) and (item[1] is not None):
                    if isinstance(item[1], int):
                        if isinstance(item[0], int):
                            return self.__data[self.__columns[item[0]]][item[1]]
                        elif isinstance(item[0], str):
                            return self.__data[item[0]][item[1]]
                        else:
                            raise TypeError
                    else:
                        raise TypeError
                elif (item[0] is not None) and (item[1] is None):
                    if isinstance(item[0], int):
                        return self.__data[self.__columns[item[0]]]
                    elif isinstance(item[0], str):
                        return self.__data[item[0]]
                    else:
                        raise TypeError
                elif (item[0] is None) and (item[1] is not None):
                    row = []
                    for var in self.__columns:
                        row.append(self.__data[var][item[1]])
                    return row
                else:
                    return ValueError
            else:
                raise ValueError

