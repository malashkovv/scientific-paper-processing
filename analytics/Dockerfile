FROM vmalashkov/apache-spark:spark-3.1.2-h3.2-py-3.8.12

COPY ./analytics/requirements.txt /usr/src/app/requirements.txt

RUN spark-pip install -r /usr/src/app/requirements.txt

COPY ./analytics/ /usr/src/app/
COPY ./core/ /usr/src/app/core/

WORKDIR /usr/src/app/

CMD ["spark-python", "cli.py"]