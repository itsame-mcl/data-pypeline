import os


class DataFrame:

    def __init__(self, data=None):
        if data is None:
            self.__groups = []
            self.__columns = []
            self.__data = {}
        elif isinstance(data, dict):
            self.__groups = []
            self.__columns = list(data.keys())
            self.__data = data
            for key in self.__columns:
                if len(self.__data[key]) < len(self):
                    for _ in range(len(self)-len(self.__data[key])):
                        self.__data[key] = self.__data[key] + [None]
        else:
            raise TypeError

    def __str__(self):
        groups = self.groups
        display = "DataFrame ID#" + str(id(self))
        display += os.linesep + "Shape : " + str(self.shape[0]) + " columns X " + str(self.shape[1]) + " lines"
        if self.__groups:
            display += os.linesep + "Groups : " + ", ".join(self.__groups)
        else:
            display += os.linesep + "Groups : None"
        lines = min(5, len(self))
        if lines > 0:
            display += os.linesep + os.linesep + "First " + str(lines) + " lines"
            display += os.linesep + "============="
            display += os.linesep + "\t"
            if self.__groups:
                display += "Group" + "\t"
            display += "\t".join(self.__columns)
            for i in range(lines):
                row = []
                for key in self.__columns:
                    row.append(str(self.__data[key][i]))
                display += os.linesep + str(i) + "\t"
                if self.__groups:
                    display += str(groups[i]) + "\t"
                display += "\t".join(row)
        return display

    def __len__(self):
        length = 0
        for key in self.__columns:
            if self.__data.get(key) is not None:
                if len(self.__data[key]) > length:
                    length = len(self.__data[key])
        return length

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
                                (0 <= item[1] < len(self)):
                            return self.__data[self.__columns[item[0]]][item[1]]
                        else:
                            raise IndexError
                    elif isinstance(item[0], str):
                        if item[0] in self.__columns and (0 <= item[1] < len(self)):
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
                if 0 <= item[1] < len(self):
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
                    if len(self) == len(value):
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
                    if len(self) == len(value):
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
                                (0 <= key[1] < len(self)):
                            self.__data[self.__columns[key[0]]][key[1]] = value
                        else:
                            raise IndexError
                    elif isinstance(key[0], str):
                        if key[0] in self.__columns and (0 <= key[1] < len(self)):
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
                                if len(self) == len(value):
                                    self.__data[self.__columns[key[0]]] = value
                                else:
                                    raise ValueError
                            else:
                                raise IndexError
                        elif isinstance(key[0], str):
                            if key[0] in self.__columns:
                                if len(self) == len(value):
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
                        if 0 <= key[1] < len(self):
                            for i in range(len(self.__columns)):
                                self.__data[self.__columns[i]][key[1]] = value[i]
                        else:
                            raise IndexError
                    else:
                        raise ValueError
                else:
                    raise TypeError
            else:
                raise ValueError
        else:
            raise TypeError

    @property
    def dict(self):
        data_dict = {}
        for key in self.__columns:
            data_dict[key] = self.__data[key]
        return data_dict

    @property
    def vars(self):
        return self.__columns

    @property
    def shape(self):
        if len(self.__columns) > 0:
            return len(self.__columns), len(self)
        else:
            return 0, 0

    @property
    def groups(self):
        if not self.__groups:
            return [0] * len(self)
        else:
            groups = []
            matchs = {}
            next_id = 1
            for i in range(len(self)):
                identifier = ""
                for key in self.__groups:
                    identifier += key + str(self.__data[key][i])
                result = matchs.get(identifier)
                if result is None:
                    matchs[identifier] = next_id
                    groups.append(next_id)
                    next_id += 1
                else:
                    groups.append(result)
            return groups

    def add_column(self, name, content=None, after=None, before=None):
        if after is not None and before is not None:
            raise ValueError
        else:
            if content is None or isinstance(content, list):
                if after is None and before is None:
                    self.__columns.append(str(name))
                elif after is not None:
                    if isinstance(after, int):
                        self.__columns.insert(after+1, str(name))
                    elif isinstance(after, str):
                        if after in self.__columns:
                            self.__columns.insert(self.__columns.index(after)+1, str(name))
                        else:
                            raise KeyError
                    else:
                        raise TypeError
                else:
                    if isinstance(before, int):
                        self.__columns.insert(before, str(name))
                    elif isinstance(before, str):
                        if before in self.__columns:
                            self.__columns.insert(self.__columns.index(before), str(name))
                        else:
                            raise KeyError
                    else:
                        raise TypeError
                if content is None:
                    content = []
                if len(content) < len(self):
                    for _ in range(len(self)-len(content)):
                        content = content + [None]
                elif len(content) > len(self):
                    for key in self.__columns:
                        if key != str(name):
                            for _ in range(len(content)-len(self.__data[key])):
                                self.__data[key] = self.__data[key] + [None]
                self.__data[str(name)] = content
            else:
                raise TypeError

    def add_row(self, content=None, after=None, before=None):
        if after is not None and before is not None:
            raise ValueError
        else:
            if content is None or isinstance(content, list):
                if content is None:
                    content = []
                if len(content) < len(self.__columns):
                    for _ in range(len(self.__columns) - len(content)):
                        content = content + [None]
                elif len(content) > len(self.__columns):
                    raise ValueError
                if after is None and before is None:
                    for i in range(len(self.__columns)):
                        self.__data[self.__columns[i]] = self.__data[self.__columns[i]] + [content[i]]
                elif after is not None:
                    if isinstance(after, int):
                        for i in range(len(self.__columns)):
                            self.__data[self.__columns[i]].insert(after+1, content[i])
                    else:
                        raise TypeError
                else:
                    if isinstance(before, int):
                        for i in range(len(self.__columns)):
                            self.__data[self.__columns[i]].insert(before, content[i])
                    else:
                        raise TypeError
            else:
                raise TypeError

    def add_group(self, name, level=None):
        if isinstance(name, str):
            if name in self.__columns:
                if name not in self.__groups:
                    if level is None:
                        self.__groups.append(name)
                    else:
                        self.__groups.insert(level, name)
                else:
                    raise KeyError
            else:
                raise KeyError
        else:
            raise TypeError

    def del_column(self, name):
        if isinstance(name, int):
            if 0 <= name < len(self.__columns):
                key = self.__columns[name]
                self.__columns.remove(key)
                del self.__data[key]
            else:
                raise IndexError
        elif isinstance(name, str):
            if name in self.__columns:
                self.__columns.remove(name)
                del self.__data[name]
            else:
                raise KeyError
        else:
            raise TypeError

    def del_row(self, index):
        if isinstance(index, int):
            if 0 <= index < len(self):
                for key in self.__columns:
                    del self.__data[key][index]
            else:
                raise IndexError
        else:
            raise TypeError

    def del_group(self, name):
        if isinstance(name, str):
            if name in self.__groups:
                self.__groups.remove(name)
            else:
                raise KeyError
        else:
            raise TypeError
