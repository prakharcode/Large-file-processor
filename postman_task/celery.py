from celery import Celery
from postman_task.utils.config_parser import config

app = Celery('postman_task',
             broker=config.BROKER_URL,
             backend=config.RESULT_BACKEND,
             include=["postman_task.destination.rdbms.postgres", "postman_task.pipelines"])

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
)

if __name__ == '__main__':
    app.start()