from abc import ABCMeta
from abc import abstractmethod


class DataVisualizationAbstract(metaclass=ABCMeta):

    @abstractmethod
    def _transform(self, *args, **kwargs):
        "Abstract method to implement  Data visualations"

    def __call__(self, *args, **kwargs):
        "This methods call transform method at class level"
        return self._transform(*args, **kwargs)
