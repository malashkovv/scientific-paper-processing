FROM python:3.7

WORKDIR /usr/src/app/

COPY data_sources/dblp/requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY core/ core/
COPY data_sources/dblp/ ./

CMD ["python", "cli.py"]
