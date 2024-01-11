ARG DE_ECR
FROM ${DE_ECR}/python:3.9-bullseye

WORKDIR /etl

COPY python_scripts python_scripts/
COPY metadata metadata/
COPY functions functions/
COPY requirements.txt .
COPY config.yaml .

RUN chmod -R 777 .

RUN apt-get update
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt

ENV PYTHONPATH .

CMD ["bash"]
