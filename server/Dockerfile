FROM python:3.9-slim-buster


COPY . /server

WORKDIR /server

RUN pip install -r requirements.txt


CMD python server.py