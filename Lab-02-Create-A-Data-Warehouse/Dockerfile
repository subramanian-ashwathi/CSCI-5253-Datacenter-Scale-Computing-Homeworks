FROM python:3.11

WORKDIR /app

RUN pip install pandas sqlalchemy psycopg2

COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py"]