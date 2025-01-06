FROM python:3.10.16

WORKDIR /usr/app

ADD ./requirements.txt ./

RUN pip install -r requirements.txt

COPY ./ ./

CMD python3 -u producer-traffic.py