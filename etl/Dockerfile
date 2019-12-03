FROM vmalashkov/apache-spark

COPY ./etl/main.py /usr/src/app/
COPY ./core/ /usr/src/app/core/

CMD ["python", "/usr/src/app/main.py"]