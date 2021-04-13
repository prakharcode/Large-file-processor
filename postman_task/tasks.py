from postman_task.celery import app
from celery import group

@app.task
def get_ingest_data_chunks_task(source,
                                destination,
                                limit,
                                offset,
                                columns):
    
    
    filename, df = s.get_data_by_chunk(limit=limit,
                                        offset=offset,
                                        columns=columns)
    
    d.put_data(data=df,
                source_type=source.source_type,
                filename=filename)