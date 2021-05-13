from Pipeline import Pipelineable
from abc import ABC, abstractmethod


class OnGroups(Pipelineable, ABC):
    """
    Abstract class for pipelinable operations on groups.

    ...

    Methods
    -------
    _operation(group_df)
        Abstract method to implement, performs the operation on a group DataFrame
    """
    @abstractmethod
    def _operation(self, group_df):
        raise NotImplementedError
