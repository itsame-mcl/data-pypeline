from abc import ABC, abstractmethod


class Pipelineable(ABC):
    """
    Abstract class for pipelinable operations.

    ...

    Methods
    -------
    apply(group_df)
        Abstract method to implement, performs the operation on a DataFrame
    """
    @abstractmethod
    def apply(self, df):
        raise NotImplementedError
