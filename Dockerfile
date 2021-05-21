FROM apache/airflow:2.0.1
COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt --no-cache-dir
