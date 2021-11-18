import os

class config:
    """Helper Class to access config variables.
    Initialzed with the start of application.
    """
    BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://localhost:5672/")
    POSTGRES_BACKEND = os.getenv("POSTGRES_BACKEND", "postgresql://postgres:admin@postgres:5432/postgres")
    RESULT_BACKEND = os.getenv("RESULT_BACKEND", "db+postgresql://postgres:admin@postgres:5432/postgres")