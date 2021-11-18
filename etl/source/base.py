from abc import ABC, abstractmethod
from etl.utils.app_logger import _get_logger


class Source(ABC):
    """A metaclass to define the source for data ingestion

    Methods
    -------
    get_data
        defines getting data from source
    
    get_data_by_chunk
        defines getting from source as chunks
    """
    def __init__(self, name:str):
        self.logger = _get_logger(name=name)
    
    @abstractmethod
    def get_data(self):
        raise NotImplementedError("Current source is not supported, raise a concern!")
    
    @abstractmethod
    def get_data_by_chunk(self):
        raise NotImplementedError("Current source is not supported, raise a concern!")