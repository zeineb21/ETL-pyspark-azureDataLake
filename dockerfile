FROM python:latest

WORKDIR /usr/app/src
COPY . .
RUN apt-get update
RUN apt-get install default-jdk -y
RUN pip install -r requirements.txt
CMD [ "python", "./loadCassandra.py", "pyspark"]
