from postman_task.destination.rdbms.postgres import Postgres
from postman_task.destination.base import Destination

class destination:
    """destination
    
    destionation()
    ------------
    Interfacr provider for destination driver
    
    Methods
    -------
    init(type:str, **kwargs)
    Return Destination type
        Params
        type: type str
    
    """
    def __init__(self):
        return

    def init(self, 
            type= "Postgres",
            **kwargs) -> Destination:
        
        if type == "Postgres":
            connection_string = kwargs.get("connection_string")
            self.driver = Postgres(connection_string=connection_string)
        
        else:
            raise NotImplementedError(f"type {type} not implemented yet.")
        return self.driver