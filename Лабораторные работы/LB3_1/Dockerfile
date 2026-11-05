FROM apache/airflow:2.10.5-python3.11
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /requirements.txt
COPY bootstrap_environment.py /bootstrap_environment.py
COPY initialize_airflow.py /initialize_airflow.py
