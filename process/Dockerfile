FROM ubuntu:20.04

RUN apt-get update -y
RUN apt-get install software-properties-common -y
RUN apt-add-repository universe
RUN apt-get update
RUN apt-get install -y python3-pip python-dev build-essential
RUN apt-get install libpangocairo-1.0-0 -y
RUN apt-get install git -y

WORKDIR /app

COPY requirements.txt .

COPY ./process .

RUN pip3 install -r requirements.txt

CMD python3.8 process.py