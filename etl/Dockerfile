FROM vmalashkov/apache-spark:spark-3.1.2-h3.2-py-3.8.12

WORKDIR /usr/src/app/

COPY ./etl/requirements.txt requirements.txt

RUN spark-pip install -r requirements.txt

COPY ./etl/ ./
COPY ./core/ ./core/

CMD ["spark-python", "cli.py"]