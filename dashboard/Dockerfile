FROM python:3.7

RUN pip install dash numpy pandas dash-bootstrap-components sqlalchemy psycopg2-binary

COPY ./dashboard/ /usr/src/app/
COPY ./core/ /usr/src/app/core/

CMD ["python", "/usr/src/app/main.py"]