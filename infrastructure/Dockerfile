#FROM python:3.11-slim-bookworm
FROM python:3.9-slim-bookworm
RUN apt-get update
#RUN apt-get install -y postgresql-client librdkafka-dev gcc make
RUN python3 -m venv /opt/venv
#RUN rm /usr/lib/python3.11/EXTERNALLY-MANAGED
#RUN pip install --no-cache abcmeta python-dotenv polars connectorx pyarrow
COPY requirements.txt .
RUN . /opt/venv/bin/activate && pip install -r requirements.txt

# clean packages
#RUN apt-get -y remove librdkafka-dev gcc make

ENTRYPOINT [ "bin/bash" ]
