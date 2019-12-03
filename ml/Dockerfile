FROM vmalashkov/apache-spark

RUN pip install numpy pandas sklearn

COPY ./ml/main.py /usr/src/app/
COPY ./core/ /usr/src/app/core/

CMD ["python", "/usr/src/app/main.py"]