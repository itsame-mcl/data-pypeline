from abc import ABC, abstractmethod


class Pipelineable(ABC):
    @abstractmethod
    def apply(self, df):
        raise NotImplementedError
