FROM apache/airflow:2.9.3
COPY requirements.txt /requirements.txt
#RUN pip install --user --upgrade pip

USER root
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt