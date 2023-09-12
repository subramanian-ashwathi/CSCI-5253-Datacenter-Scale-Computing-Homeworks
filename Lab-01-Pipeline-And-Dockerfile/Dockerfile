FROM python:3.8

WORKDIR /app
COPY pipeline.py pipeline_c.py

RUN pip install pandas

ENTRYPOINT [ "python", "pipeline_c.py", "http://shelterdata.s3.amazonaws.com/shelter1000.csv", "target.csv"]