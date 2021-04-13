import pandas as pd
from postman_task.celery import app
from sqlalchemy import create_engine
from postman_task.destination.rdbms.operations import upsert_wrap
from postman_task.destination.rdbms.base import RDBMSDestination

class Postgres(RDBMSDestination):
    """Derived class from RDBMSDestination to get data into Postgres.
    Using SQLAlchemy backend.

    
    Initialize
    ----------
    Postgres(connection_str:str)
    connection_str:string
        connection in sqlalchemy format to support integration

    Methods
    -------
    put_data_by_chunk()
        Returns None
        Responsible to extract data and put in Postgres (in chunks).
    
    put_data()
        Returns None
        Responsible to extract data and put in Postgres.
    """
    def __init__(self, connection_string:str):
        super().__init__(name=__name__,
                        connection_string = connection_string)
        self.logger.info(f"Postgres connection -> [{connection_string}]")
    
    @property
    def connection_string(self) -> str:
        return self.connection_string
    
    @property
    def type(self) -> str:
        return "Postgres"
    
    def put_data(self, data, source_type, filename):
        """put_data_by_chunk()
        Returns None
        Responsible to extract data and put in Postgres (in chunks).
        """
        
        if source_type == "CSV":
            try:
                data.to_sql(filename, self.get_db_conn(), index=False)
            except Exception as e:
                self.logger.error(f"Failed to ingest -> {e}", exc_info= True)
        else:
            raise NotImplementedError("The {source_type} is not ingestable in Postgres.")
    
    def put_data_by_chunk(self, data, source_type, table_name):
        """put_data()
        Returns None
        Responsible to extract data and put in Postgres.
        """
        if source_type == "CSV":
            # todo
            upsert = upsert_wrap(self.get_meta_conn())
            try:
                data.to_sql(table_name, 
                            self.get_db_conn(), 
                            if_exists='append',
                            index=False)
            
            except Exception as e:
                self.logger.error(f"Failed to ingest -> {e}", exc_info=True)
        else:
            raise NotImplementedError(f"The {source_type} is not ingestable in Postgres.")