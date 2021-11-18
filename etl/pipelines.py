import os
from celery import group
from etl.celery import app
from etl.source import source
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from etl.destination import destination
from etl.utils.app_logger import _get_logger
from etl.tasks import get_ingest_data_chunks_task

class data_ingestion_pipeline:
    """pipeline
    Abstraction over celery tasks to provide a feel of DAG like airflow.
    
    pipeline(source_conf, destination_conf)
    -------------------------
    source_conf: dict
        Keys:
            type: str -> Type of source
            location: str -> connection to source
    destination_conf: dict
        Keys:
            type: str -> Type of destination
            location: str -> connection to destination
    
    Methods
    -------
    ingest_data()
    to ingest data direct from source to destination without chunking
    
    ingest_data_chunk()
    to ingest data direct from source to destination with chunking
    
    ingest_data_chunks( chunksize, table_name=None):
    parallel driver for ingest data direct from source to destination with chunking
    
    update_data_chunk()
    to update data direct from source to destination with chunking
    
    update_data_chunk()
    to update data direct from source to destination with chunking
    """
    def __init__(self,
                source_conf:dict,
                destination_conf:dict):
        self.logger = _get_logger(name = __name__)
        
        self.source_type = source_conf.pop("type")
        self.source_conf = source_conf
        
        self.destination_type = destination_conf.pop("type")
        self.destination_conf = destination_conf
        
        self.source = source().init(type=self.source_type, **source_conf)
        self.destination = destination().init(type=self.destination_type, **destination_conf)
    
    @app.task(bind=True)
    def ingest_data(self, operation="dedup_last"):
        filename, df = self.source.get_data()
        
        if operation == "dedup_last":
            dupes = df[df.duplicated(primary_keys)]
            if len(dupes):
                os.makedirs("dump", exist_ok=True)
                dupes.to_csv(f"dump/{filename}_{offset}.csv", index=False)
            df.drop_duplicates(primary_keys, keep="first", inplace=True)
        
        self.destination.put_data(data=df,
                            source_type=source.type,
                            filename=filename)

    @app.task(bind=True)
    def ingest_data_chunk(self, source_conf, 
                        destination_conf,
                        limit, offset,
                        operation="dedup_last",
                        columns=None,
                        **kwargs):
        
        source_type = source_conf.pop("type")
        source_conf = source_conf
        
        destination_type = destination_conf.pop("type")
        destination_conf = destination_conf
        
        s = source().init(type=source_type, **source_conf)
        d = destination().init(type=destination_type, **destination_conf)

        filename, df = s.get_data_by_chunk(limit=limit,
                                        offset=offset,
                                        columns=columns)

        table_name = kwargs.get("table_name", filename)
        table_name = table_name if table_name else filename
        primary_keys = kwargs.get("primary_keys", "sku,name")
        primary_keys = primary_keys.split(",")

        engg = d.get_engine()
        session_factory = sessionmaker(bind=engg, autocommit=True)
        Session = scoped_session(session_factory)
        sess = Session()
        
        # custom
        sess.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} 
                    (sku VARCHAR, name VARCHAR, description TEXT, UNIQUE(sku, name));
                     """)

        if operation == "dedup_last":
            dupes = df[df.duplicated(primary_keys)]
            if len(dupes):
                os.makedirs("dump", exist_ok=True)
                dupes.to_csv(f"dump/{filename}_{offset}.csv", index=False)
            df.drop_duplicates(primary_keys, keep="first", inplace=True)
        temp_table = f"{table_name}_temp_{offset}"
        d.put_data_by_chunk(data=df,
            source_type=s.type,
            table_name=temp_table)
        
        sess.execute(f"""INSERT INTO {table_name} (name, sku, description)
                        (SELECT {temp_table}.name, {temp_table}.sku, {temp_table}.description 
                        FROM {table_name} RIGHT JOIN {temp_table} ON 
                        ({table_name}.sku = {temp_table}.sku AND 
                        {table_name}.name = {temp_table}.name)
                        WHERE ({table_name}.name IS NULL AND {table_name}.sku IS NULL))
                        ON CONFLICT (name, sku) DO NOTHING;""")
        sess.execute(f"""DROP TABLE IF EXISTS {temp_table};""")
    
    def ingest_data_chunks(self, chunksize, table_name=None):
        results = []
        offset = 0
        limit = chunksize

        self.source_conf.update({"type":self.source.type})
        self.destination_conf.update({"type":self.destination.type})
        _, df = self.source.get_data_by_chunk(limit=limit,
                                            offset=offset)
        columns = list(df.columns)

        while len(df):
            self.logger.info(f"Processing chunk {offset/limit}")
            task = self.ingest_data_chunk.s(source_conf=self.source_conf,
                                            destination_conf=self.destination_conf,
                                            limit=limit,
                                            offset=offset,
                                            columns=columns,
                                            primary_keys="sku,name",
                                            table_name=table_name)
            results.append(task.delay())
            offset += limit
            _, df = self.source.get_data_by_chunk(limit=limit,
                                                offset=offset)
        return results

    @app.task(bind=True)
    def update_data_chunk(self, source_table,
                        source_conf, 
                        destination_conf,
                        limit, offset,
                        operation="dedup_last",
                        columns=None,
                        **kwargs):
        source_type = source_conf.pop("type")
        source_conf = source_conf

        destination_type = destination_conf.pop("type")
        destination_conf = destination_conf

        s = source().init(type=source_type, **source_conf)
        d = destination().init(type=destination_type, **destination_conf)

        filename, df = s.get_data_by_chunk(limit=limit,
                                        offset=offset,
                                        columns=columns)

        table_name = f"{filename}_temp_{offset}"
        primary_keys = kwargs.get("primary_keys", "sku,name")
        primary_keys = primary_keys.split(",")
        
        if operation == "dedup_last":
            dupes = df[df.duplicated(primary_keys)]
            if len(dupes):
                os.makedirs("dump", exist_ok=True)
                dupes.to_csv(f"dump/{filename}_{offset}.csv", index=False)
            df.drop_duplicates(primary_keys, keep="first", inplace=True)
            
        d.put_data_by_chunk(data=df,
            source_type=s.type,
            table_name=table_name)

        engg = d.get_engine()
        session_factory = sessionmaker(bind=engg, autocommit=True)
        Session = scoped_session(session_factory)
        sess = Session()
        
        # custom queries
        # todo move away into operations.
        sess.execute(f"""INSERT INTO {source_table} (name, sku, description)
                        SELECT {table_name}.name, {table_name}.sku, {table_name}.description 
                        FROM {source_table} RIGHT JOIN {table_name} ON 
                        ({source_table}.sku = {table_name}.sku AND 
                        {source_table}.name = {table_name}.name)
                        WHERE ({source_table}.name IS NULL AND {source_table}.sku IS NULL);""")

        sess.execute(f"""CREATE TABLE {table_name}_c AS 
                    (SELECT {table_name}.name, {table_name}.sku, {table_name}.description
                    FROM {source_table} RIGHT JOIN {table_name} ON 
                    ({source_table}.sku = {table_name}.sku AND
                    {source_table}.name = {table_name}.name)
                    WHERE ({source_table} IS NOT NULL AND {source_table} IS NOT NULL));""")
        
        sess.execute(f"""UPDATE {source_table}
                    SET description = {table_name}_c.description
                    FROM {table_name}_c
                    WHERE {source_table}.name = {table_name}_c.name AND 
                    {source_table}.sku = {table_name}_c.sku;""")

        
        sess.execute(f"drop table {table_name};")
        sess.execute(f"""DROP TABLE {table_name}_c;""")

    def update_data_chunks(self, source_table, chunksize, **kwargs):
        results = []
        offset = 0
        limit = chunksize
        
        self.source_conf.update({"type":self.source.type})
        self.destination_conf.update({"type":self.destination.type})
        _, df = self.source.get_data_by_chunk(limit=limit,
                                            offset=offset)
        columns = list(df.columns)
        
        while len(df):
            self.logger.info(f"Processing chunk {offset/limit}")
            task = self.update_data_chunk.s(source_table=source_table,
                                            source_conf=self.source_conf,
                                            destination_conf=self.destination_conf,
                                            limit=limit,
                                            offset=offset,
                                            columns=columns,
                                            primary_keys="sku,name")
            results.append(task.delay())
            offset += limit
            _, df = self.source.get_data_by_chunk(limit=limit,
                                                offset=offset)
        return results
    
    
class data_aggregation_pipeline:
    def __init__(self):
        pass
    
    @app.task(bind=True)
    def operation(self,  destination_conf:dict, source_table:str, by:str) -> None:
        destination_type = destination_conf.pop("type")
        d = destination().init(type=destination_type, **destination_conf)
        
        engg = d.get_engine()
        session_factory = sessionmaker(bind=engg, autocommit=True)
        Session = scoped_session(session_factory)
        sess = Session()
        
        delete_if_exists_query = f"""DROP TABLE IF EXISTS {source_table}_agg"""
        
        sess.execute(delete_if_exists_query)
        
        create_agg_table = f"""CREATE TABLE {source_table}_agg AS 
        (SELECT name, count(*) as no_of_products FROM {source_table} GROUP by {by});"""
        
        sess.execute(create_agg_table)
    
    def get_aggregate_table(self, destination_conf:dict, source_table:str, by:str) -> None:
        task = self.operation.s(destination_conf=destination_conf,
                       source_table=source_table,
                       by=by)
        result = task.delay()
        return result
