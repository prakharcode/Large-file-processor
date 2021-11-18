from abc import ABC, abstractmethod
from etl.utils.app_logger import _get_logger


class Destination(ABC):
    """
    A metaclass to define the destination for data ingestion

    Methods
    -------
    put_data
        defines putting data into destination
    
    put_data_by_chunk
        defines putting data into destination by chunks
    """
    def __init__(self, name:str):
        self.logger = _get_logger(name=name)
    
    @abstractmethod
    def put_data(self):
        raise NotImplementedError("Current source is not supported, raise a concern!")
    
    @abstractmethod
    def put_data_by_chunk(self):
        raise NotImplementedError("Current source is not supported, raise a concern!")