from etl.destination.base import Destination
from etl.utils.app_logger import _get_logger
from sqlalchemy import create_engine, MetaData, Table

class RDBMSDestination(Destination):
    """Derived class from RDBMSDestination to get data into Postgres.
    Using SQLAlchemy backend.

    
    Initialize
    ----------
    Postgres(connection_str:str)
    connection_str:string
        connection in sqlalchemy format to support integration

    Methods
    -------
    get_engine()
        Returns sqlalchemy engine
    
    get_db_conn()
        Returns sqlalchemy connection
    
    get_meta_conn()
        Returns sqlalchemy metadata for a database.
    """
    def __init__(self, name, connection_string:str):
        super().__init__(name=name)
        self.sql_conn_string = connection_string

    def get_engine(self):
        """get_engine()
        Returns sqlalchemy engine
        """
        try:
            self.engine = create_engine(self.sql_conn_string)
        except Exception as e:
            self.logger.info('Connection to sql backend failed -- {}'.format(e))
            return None
        return self.engine

    def get_db_conn(self):
        """get_db_conn()
        Returns sqlalchemy connection
        """
        sql_engine = self.get_engine()
        conn_to_db = sql_engine.connect()
        return conn_to_db

    def get_meta_conn(self):
        """get_meta_conn()
        Returns sqlalchemy metadata for a database.
        """
        engine = self.get_engine()
        meta = MetaData(engine)
        return meta