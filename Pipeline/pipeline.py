from Pipeline import Pipelineable
from copy import deepcopy


class Pipeline(Pipelineable):
    """
    A Pipeline of pipelineable operations to apply on a DataFrame.

    Is itself a pipelinable operation.

    ...

    Attributes
    ----------
    self.__operations : list
        List of pipelineable operations to apply on a DataFrame

    Methods
    -------
    __init__(*operations)
        Create a Pipeline with the specified operations
    add_operations(*operations)
        Add new operations to the Pipeline after it's creation
    operations : list
        Returns the list of currently specified operations
    del_operations(*operations)
        Remove operations to the Pipeline after it's creation
    apply(df) : DataFrame
        Apply the operations of the PipeLine to the DataFrame df and returns the final DataFrame
    _secure_add_operations(operations):
        Handles the pipelineable checking when adding operations
    """
    def __init__(self, *operations):
        self.__operations = []
        self.__secure_add_operations(operations)

    def add_operations(self, *operations):
        self.__secure_add_operations(operations)

    @property
    def operations(self):
        return self.__operations

    def del_operations(self, *operations):
        for operation in operations:
            if issubclass(type(operation), Pipelineable):
                if operation in self.__operations:
                    self.__operations.remove(operation)
                else:
                    raise ValueError
            else:
                raise TypeError

    def apply(self, df):
        pipeline_df = deepcopy(df)
        for operation in self.__operations:
            pipeline_df = operation.apply(pipeline_df)
        return pipeline_df

    def __secure_add_operations(self, operations):
        for operation in operations:
            if issubclass(type(operation), Pipelineable):
                self.__operations.append(operation)
            else:
                raise TypeError
