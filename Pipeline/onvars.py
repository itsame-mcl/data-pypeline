from abc import ABC
from Pipeline.pipelineable import Pipelineable


class OnVars(ABC, Pipelineable):
    def __init__(self, arg_vars):
        self.__vars = []
        self.__secure_add_vars(arg_vars)

    def add_vars(self, arg_vars):
        self.__secure_add_vars(arg_vars)

    def get_vars(self):
        return self.__vars

    def del_vars(self, arg_vars):
        if isinstance(arg_vars, str):
            if arg_vars in self.__vars:
                self.__vars.remove(arg_vars)
            else:
                raise ValueError
        elif isinstance(arg_vars, list) or isinstance(arg_vars, tuple) or isinstance(arg_vars, set):
            for var in arg_vars:
                if isinstance(var, str):
                    if var in self.__vars:
                        self.__vars.remove(var)
                    else:
                        raise ValueError
                else:
                    raise TypeError
        else:
            raise TypeError

    def __secure_add_vars(self, arg_vars):
        if isinstance(arg_vars, str):
            self.__vars.append(arg_vars)
        elif isinstance(arg_vars, list) or isinstance(arg_vars, tuple) or isinstance(arg_vars, tuple):
            for var in arg_vars:
                if isinstance(var, str):
                    self.__vars.append(var)
                else:
                    raise TypeError
        else:
            raise TypeError
