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
            else:
                raise ValueError
        elif isinstance(item, int):
            if 0 <= item < len(self.__columns):
                return self.__data[self.__columns[item]]
            else:
                raise OverflowError
        elif isinstance(item, tuple):
            if len(item) == 1:
                if isinstance(item[0], str):
                    if item[0] in self.__columns:
                        return self.__data[item[0]]
                    else:
                        raise ValueError
                elif isinstance(item[0], int):
                    if 0 <= item[0] < len(self.__columns):
                        return self.__data[self.__columns[item[0]]]
                    else:
                        raise OverflowError
            elif len(item) == 2:
                if (item[0] is not None) and (item[1] is not None):
                    if isinstance(item[1], int):
                        if isinstance(item[0], int):
                            if (0 <= item[0] < len(self.__columns)) and \
                                    (0 <= item[1] < len(self.__data[self.__columns[0]])):
                                return self.__data[self.__columns[item[0]]][item[1]]
                            else:
                                raise OverflowError
                        elif isinstance(item[0], str):
                            if item[0] in self.__columns and (0 <= item[1] < len(self.__data[self.__columns[0]])):
                                return self.__data[item[0]][item[1]]
                            else:
                                raise OverflowError
                        else:
                            raise TypeError
                    else:
                        raise TypeError
                elif (item[0] is not None) and (item[1] is None):
                    if isinstance(item[0], int):
                        if 0 <= item[0] < len(self.__columns):
                            return self.__data[self.__columns[item[0]]]
                        else:
                            raise OverflowError
                    elif isinstance(item[0], str):
                        if item[0] in self.__columns:
                            return self.__data[item[0]]
                        else:
                            raise OverflowError
                    else:
                        raise TypeError
                elif (item[0] is None) and (item[1] is not None):
                    if 0 <= item[1] < len(self.__data[self.__columns[0]]):
                        row = []
                        for var in self.__columns:
                            row.append(self.__data[var][item[1]])
                        return row
                    else:
                        return OverflowError
                else:
                    return ValueError
            else:
                raise ValueError

    def __setitem__(self, key, value):
        if isinstance(key, str):
            if isinstance(value, list):
                if key in self.__columns:
                    if len(self.__data[key]) == len(value):
                        self.__data[key] = value
                    else:
                        raise OverflowError
                else:
                    raise ValueError
            else:
                raise TypeError
        elif isinstance(key, int):
            if isinstance(value, list):
                if 0 <= key < len(self.__columns):
                    self.__data[self.__columns[key]] = value
                else:
                    raise OverflowError
            else:
                raise ValueError
        elif isinstance(key, tuple):
            if len(key) == 1:
                if isinstance(key[0], str):
                    if isinstance(value, list):
                        if key[0] in self.__columns:
                            if len(self.__data[key[0]]) == len(value):
                                self.__data[key[0]] = value
                            else:
                                raise OverflowError
                        else:
                            raise ValueError
                    else:
                        raise TypeError
                elif isinstance(key[0], int):
                    if isinstance(value, list):
                        if 0 <= key[0] < len(self.__columns):
                            self.__data[self.__columns[key[0]]] = value
                        else:
                            raise OverflowError
                    else:
                        raise ValueError
            elif len(key) == 2:
                if (key[0] is not None) and (key[1] is not None):
                    if isinstance(key[1], int):
                        if isinstance(key[0], int):
                            if (0 <= key[0] < len(self.__columns)) and \
                                    (0 <= key[1] < len(self.__data[self.__columns[0]])):
                                self.__data[self.__columns[key[0]]][key[1]] = value
                            else:
                                raise OverflowError
                        elif isinstance(key[0], str):
                            if key[0] in self.__columns and (0 <= key[1] < len(self.__data[self.__columns[0]])):
                                self.__data[key[0]][key[1]] = value
                            else:
                                raise OverflowError
                        else:
                            raise TypeError
                    else:
                        raise TypeError
                elif (key[0] is not None) and (key[1] is None):
                    if isinstance(value, list):
                        if len(self.__data[self.__columns[0]]) == len(value):
                            if isinstance(key[0], int):
                                if 0 <= key[0] < len(self.__columns):
                                    self.__data[self.__columns[key[0]]] = value
                                else:
                                    raise OverflowError
                            elif isinstance(key[0], str):
                                if key[0] in self.__columns:
                                    self.__data[key[0]] = value
                                else:
                                    raise OverflowError
                            else:
                                raise TypeError
                        else:
                            raise ValueError
                    else:
                        raise TypeError
                elif (key[0] is None) and (key[1] is not None):
                    if isinstance(value, list):
                        if len(self.__data[self.__columns[0]]) == len(value):
                            if 0 <= key[1] < len(self.__data[self.__columns[0]]):
                                for i in range(len(self.__columns)):
                                    self.__data[self.__columns[i]][key[1]] = value[i]
                            else:
                                return OverflowError
                        else:
                            raise ValueError
                    else:
                        raise TypeError
                else:
                    raise TypeError
            else:
                raise ValueError
        else:
            raise TypeError
