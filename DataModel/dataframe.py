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
        if isinstance(item, int):
            if 0 <= item < len(self.__columns):
                return self.__data[self.__columns[item]]
            else:
                raise IndexError
        elif isinstance(item, str):
            if item in self.__columns:
                return self.__data[item]
            else:
                raise KeyError
        elif isinstance(item, tuple):
            if (len(item) == 2) and (item[0] is not None) and (item[1] is not None):
                if isinstance(item[1], int):
                    if isinstance(item[0], int):
                        if (0 <= item[0] < len(self.__columns)) and \
                                (0 <= item[1] < len(self.__data[self.__columns[0]])):
                            return self.__data[self.__columns[item[0]]][item[1]]
                        else:
                            raise IndexError
                    elif isinstance(item[0], str):
                        if item[0] in self.__columns and (0 <= item[1] < len(self.__data[self.__columns[0]])):
                            return self.__data[item[0]][item[1]]
                        else:
                            raise KeyError
                    else:
                        raise TypeError
                else:
                    raise TypeError
            elif (len(item) == 1) and (item[0] is not None) or\
                    (len(item) == 2) and (item[0] is not None) and (item[1] is None):
                if isinstance(item[0], int):
                    if 0 <= item[0] < len(self.__columns):
                        return self.__data[self.__columns[item[0]]]
                    else:
                        raise IndexError
                elif isinstance(item[0], str):
                    if item[0] in self.__columns:
                        return self.__data[item[0]]
                    else:
                        raise KeyError
                else:
                    raise TypeError
            elif (len(item) == 2) and (item[0] is None) and (item[1] is not None):
                if 0 <= item[1] < len(self.__data[self.__columns[0]]):
                    row = []
                    for var in self.__columns:
                        row.append(self.__data[var][item[1]])
                    return row
                else:
                    return IndexError
            else:
                return ValueError
        else:
            raise TypeError

    def __setitem__(self, key, value):
        if isinstance(key, int):
            if isinstance(value, list):
                if 0 <= key < len(self.__columns):
                    if len(self.__data[self.__columns[key]]) == len(value):
                        self.__data[self.__columns[key]] = value
                    else:
                        raise ValueError
                else:
                    raise IndexError
            else:
                raise TypeError
        elif isinstance(key, str):
            if isinstance(value, list):
                if key in self.__columns:
                    if len(self.__data[key]) == len(value):
                        self.__data[key] = value
                    else:
                        raise ValueError
                else:
                    raise KeyError
            else:
                raise TypeError
        elif isinstance(key, tuple):
            if (len(key) == 2) and (key[0] is not None) and (key[1] is not None):
                if isinstance(key[1], int):
                    if isinstance(key[0], int):
                        if (0 <= key[0] < len(self.__columns)) and \
                                (0 <= key[1] < len(self.__data[self.__columns[0]])):
                            self.__data[self.__columns[key[0]]][key[1]] = value
                        else:
                            raise IndexError
                    elif isinstance(key[0], str):
                        if key[0] in self.__columns and (0 <= key[1] < len(self.__data[self.__columns[0]])):
                            self.__data[key[0]][key[1]] = value
                        else:
                            raise KeyError
                    else:
                        raise TypeError
                else:
                    raise TypeError
            elif (len(key) == 1) and (key[0] is not None) or\
                    (len(key) == 2) and (key[0] is not None) and (key[1] is None):
                if isinstance(value, list):
                    if len(self.__data[self.__columns[0]]) == len(value):
                        if isinstance(key[0], int):
                            if 0 <= key[0] < len(self.__columns):
                                if len(self.__data[self.__columns[key[0]]]) == len(value):
                                    self.__data[self.__columns[key[0]]] = value
                                else:
                                    raise ValueError
                            else:
                                raise IndexError
                        elif isinstance(key[0], str):
                            if key[0] in self.__columns:
                                if len(self.__data[key[0]]) == len(value):
                                    self.__data[key[0]] = value
                                else:
                                    raise ValueError
                            else:
                                raise KeyError
                        else:
                            raise TypeError
                    else:
                        raise ValueError
                else:
                    raise TypeError
            elif (len(key) == 2) and (key[0] is None) and (key[1] is not None):
                if isinstance(value, list):
                    if len(self.__columns) == len(value):
                        if 0 <= key[1] < len(self.__data[self.__columns[0]]):
                            for i in range(len(self.__columns)):
                                self.__data[self.__columns[i]][key[1]] = value[i]
                        else:
                            return IndexError
                    else:
                        raise ValueError
                else:
                    raise TypeError
            else:
                raise ValueError
        else:
            raise TypeError
