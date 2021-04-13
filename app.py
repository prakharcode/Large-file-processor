import sys
from celery import group
from postman_task.utils.config_parser import config
from postman_task.utils.app_logger import _get_logger
from postman_task.pipelines import ( data_ingestion_pipeline, 
            data_aggregation_pipeline )

LOGGER = _get_logger(__name__)

destination_conf = {"type":"Postgres", "connection_string":config.POSTGRES_BACKEND}

# ingestion
if sys.argv[1] == "ingest":
    try:
        csv_path = sys.argv[2]
    except:
        LOGGER.error("enter valid file name")
    source_conf = {"type":"CSV", "csv_location":csv_path}
    workflow = data_ingestion_pipeline(source_conf, destination_conf)
    results = workflow.ingest_data_chunks(chunksize=100000)
    print(all(results))

# update
if sys.argv[1] == "update":
    try:
        csv_path = sys.argv[2]
    except:
        LOGGER.error("enter valid file name")
    source_conf = {"type":"CSV", "csv_location":csv_path}
    workflow = data_ingestion_pipeline(source_conf, destination_conf)
    results = workflow.update_data_chunks(source_table="products", chunksize=100000)
    print(all(results))

# aggregate
if sys.argv[1] == "aggregate":
    try:
        source_table = sys.argv[2]
        by = sys.argv[3]
    except:
        LOGGER.error("invalid table name or group by key")
    
    workflow = data_aggregation_pipeline()
    workflow.get_aggregate_table(destination_conf=destination_conf,
                                source_table=source_table,
                                by=by)