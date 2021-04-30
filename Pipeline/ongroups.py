from Pipeline import Pipelineable
from abc import ABC, abstractmethod


class OnGroups(Pipelineable, ABC):
    @abstractmethod
    def _operation(self, group_df):
        raise NotImplementedError
