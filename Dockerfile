FROM python:3.7.6-buster

ENV POSTGRES_BACKEND=postgresql://postgres:admin@postgres:5432/postgres
ENV CELERY_BROKER_URL=amqp://rabbitmq:5672/
ENV RESULT_BACKEND=db+postgresql://postgres:admin@postgres:5432/postgres
RUN apt-get update && apt-get install -y postgresql python-psycopg2 libpq-dev gcc musl-dev
WORKDIR .
COPY . .
RUN pip install -r requirements.txt