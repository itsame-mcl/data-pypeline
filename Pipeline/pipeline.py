from Pipeline.pipelineable import Pipelineable
from copy import deepcopy


class Pipeline:
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
