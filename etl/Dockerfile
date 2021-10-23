FROM vmalashkov/apache-spark:spark-3.1.2-h3.2-py-3.8.12

COPY ./etl/main.py /usr/src/app/
COPY ./core/ /usr/src/app/core/

CMD ["python", "/usr/src/app/main.py"]