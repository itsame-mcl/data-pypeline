from abc import ABC
from Pipeline.pipelineable import Pipelineable


class OnVars(Pipelineable, ABC):
    def __init__(self, arg_vars):
        self._vars = []
        self._secure_add_vars(arg_vars)

    def add_vars(self, arg_vars):
        self._secure_add_vars(arg_vars)

    @property
    def get_vars(self):
        return self._vars

    def del_vars(self, arg_vars):
        if isinstance(arg_vars, str):
            if arg_vars in self._vars:
                self._vars.remove(arg_vars)
            else:
                raise ValueError
        elif isinstance(arg_vars, list) or isinstance(arg_vars, tuple) or isinstance(arg_vars, set):
            for var in arg_vars:
                if isinstance(var, str):
                    if var in self._vars:
                        self._vars.remove(var)
                    else:
                        raise ValueError
                else:
                    raise TypeError
        else:
            raise TypeError

    def _secure_add_vars(self, arg_vars):
        if isinstance(arg_vars, str):
            self._vars.append(arg_vars)
        elif isinstance(arg_vars, list) or isinstance(arg_vars, tuple) or isinstance(arg_vars, tuple):
            for var in arg_vars:
                if isinstance(var, str):
                    self._vars.append(var)
                else:
                    raise TypeError
        else:
            raise TypeError
