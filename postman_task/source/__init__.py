from postman_task.source.base import Source
from postman_task.source.csv import CSVsource
import pandas as pd

class source:
    def __init__(self,
                ) -> None:
        return

    def init(self,
            type= "CSV",
            **kwargs) -> Source:
        
        if type == "CSV":
            csv_location = kwargs.get("csv_location")
            self.driver = CSVsource(csv_location=csv_location)

        else:
            raise NotImplementedError(f"type {type} not implemented yet.")
        
        return self.driver