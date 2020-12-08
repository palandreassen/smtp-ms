FROM python:3-alpine

RUN apk update

RUN pip install --upgrade pip
RUN apk --update add build-base libffi-dev libressl-dev python3 python3-dev py3-pip
RUN pip install cryptography

COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
COPY ./service /service

EXPOSE 5000

CMD ["python3", "./service/main.py"]