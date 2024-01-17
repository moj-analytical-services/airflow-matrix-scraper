FROM python:3.9

WORKDIR /etl

COPY python_scripts .
COPY metadata metadata/
COPY requirements.txt .

RUN chmod -R 777 .

RUN apt-get update
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["bash"]