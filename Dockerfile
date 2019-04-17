FROM python:3.7



RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libatlas-base-dev gfortran


#  RUN apt-get install -y software-properties-common \
#     && add-apt-repository ppa:openjdk-r/ppa \
#     && apt-get update \
#     && apt-get install -y openjdk-8-jdk ca-certificates-java \
#     && update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

 RUN apt-get install -y software-properties-common \
    && apt-get install -y openjdk-8-jdk ca-certificates-java


RUN mkdir -p /opt/pandas/build/

COPY requirements.txt /opt/pandas/build/requirements.txt

RUN pip install -r /opt/pandas/build/requirements.txt



RUN mkdir -p /airflow/xcom
COPY python_scripts /
COPY metadata metadata/

CMD ["bash"]