FROM python:3.9-slim-buster

COPY . /client

WORKDIR /client

RUN pip install -r requirements.txt

CMD python client.py