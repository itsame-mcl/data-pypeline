from abc import ABC
from Pipeline import Pipelineable


class OnVars(Pipelineable, ABC):
    """
    Abstract class for pipelinable operations taking variable names as parameters.

    ...

    Attributes
    ----------
    self._vars : list
        List of variables to use on the apply or _operation method

    Methods
    -------
    __init__(*on_vars)
        Base constructor for child classes, handling the variables names specification
    add_vars(*on_vars)
        Add new variables to the object after it's creation
    vars : list
        Returns the list of currently specified variables
    del_vars(*on_vars)
        Remove variables to the object after it's creation
    _secure_add_vars(on_vars):
        Handles the type checking on adding variables operations
    """
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
