FROM vmalashkov/apache-spark

RUN pip install numpy pandas sklearn sqlalchemy psycopg2-binary

COPY ./analytics/ /usr/src/app/
COPY ./core/ /usr/src/app/core/

CMD ["python", "/usr/src/app/classifier_training.py"]