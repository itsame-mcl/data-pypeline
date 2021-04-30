from abc import ABC
from Pipeline import Pipelineable


class OnVars(Pipelineable, ABC):
    def __init__(self, *on_vars):
        self._vars = []
        self._secure_add_vars(on_vars)

    def add_vars(self, *on_vars):
        self._secure_add_vars(on_vars)

    @property
    def vars(self):
        return self._vars

    def del_vars(self, *on_vars):
        for var in on_vars:
            if isinstance(var, str):
                if var in self._vars:
                    self._vars.remove(var)
                else:
                    raise ValueError
            else:
                raise TypeError

    def _secure_add_vars(self, on_vars):
        for var in on_vars:
            if isinstance(var, str):
                self._vars.append(var)
            else:
                raise TypeError
