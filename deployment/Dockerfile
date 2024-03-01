FROM apache/airflow:2.8.2-python3.8
RUN python -m pip install --upgrade pip
COPY requirements.txt /
RUN pip install -U --use-deprecated=legacy-resolver --no-cache-dir -r /requirements.txt
RUN pip install pyvim
RUN pip install awscli
RUN pip install slack-webhook
