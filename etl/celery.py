from celery import Celery
from etl.utils.config_parser import config

app = Celery('etl',
             broker=config.BROKER_URL,
             backend=config.RESULT_BACKEND,
             include=["etl.destination.rdbms.postgres", "etl.pipelines"])

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
)

if __name__ == '__main__':
    app.start()