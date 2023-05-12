FROM apache/spark-py:v3.1.3

USER root
#ENV PYSPARK_MAJOR_PYTHON_VERSION=3
RUN apt-get update
RUN apt install -y python3 python3-pip
RUN pip3 install --upgrade pip setuptools --user
RUN rm -r /root/.cache && rm -rf /var/cache/apt/*

WORKDIR /opt/application
COPY requirements.txt .

RUN pip3 install -r requirements.txt --user

COPY mysql-connector-java-8.0.30.jar /opt/spark/jars
COPY faking-data-scripts.py /faking-data-scripts.py
COPY etl-scripts.py /etl-scripts.py

# ENTRYPOINT ["sh","/entrypoint.sh"]
# CMD ["python", "faking-data-scripts.py"]
CMD ["python", "etl-scripts.py"]