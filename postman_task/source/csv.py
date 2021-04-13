from .base import Source
from pathlib import Path
import pandas as pd

class CSVsource(Source):
    """Derived class from Source to get data from CSV.
    Using pandas.

    
    Initialize
    ----------
    CSVsource(csv_location:str)
    csv_location:string
        location to the csv file.

    Methods
    -------
    get_data() -> str, pd.DataFrame
        Returns str, pd.DataFrame
        Responsible to get data from csv into dataframe.
    
    get_data_by_chunk(limit:int, offset:int) -> str, pd.DataFrame
        Returns str, pd.DataFramene
        Responsible to get data from csv into data with limit number
        of rows and starting from offset.
    """
    def __init__(self,
                csv_location:str = None) -> None:
        super().__init__(name=__name__)
        self.csv_location = Path(csv_location)
        self.logger.info(f"Getting csv from -> [{self.csv_location}]")

    @property
    def location(self) -> str:
        return self.csv_location

    @property
    def type(self) -> str:
        return "CSV"

    def get_data(self) -> (str, pd.DataFrame):
        """get_data() -> str, pd.DataFrame
        Returns str, pd.DataFrame
        Responsible to get data from csv into dataframe.
        """
        filename = self.csv_location.stem
        try:
            df = pd.read_csv(self.csv_location)
            return filename, df
        except Exception as e:
            self.logger.info("Could not read df, try again.")

    def get_data_by_chunk(self, limit:int, offset:int, columns=None) -> (str, pd.DataFrame):
        """get_data_by_chunk(limit:int, offset:int) -> str, pd.DataFrame
        Returns str, pd.DataFramene
        Responsible to get data from csv into data with limit number
        of rows and starting from offset.
        """
        filename = self.csv_location.stem
        self.logger.info(f"Reading data from csv [{filename}] from {offset} - {offset+limit} rows")
        try:
            chunk =  pd.read_csv(self.csv_location,
                            skiprows= offset,
                            nrows= limit,
                            names=columns)
            return filename, chunk
        except Exception as e:
            self.logger.error("Could not read chunk [{chunk_name}].")