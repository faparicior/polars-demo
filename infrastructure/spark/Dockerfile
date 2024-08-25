FROM apache/spark:latest

# Install additional dependencies if required
RUN apt-get update && \
    apt-get install -y python3-pip

# Install required Python packages
COPY requirements.txt requirements.txt
#RUN pip install -r requirements.txt
