FROM apache/airflow:latest

RUN pip install --no-cache-dir apache-airflow-providers-docker \
    pandas \
    sqlalchemy \
    pymysql \
    google-cloud-bigquery \
    google-auth \