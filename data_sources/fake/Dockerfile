FROM python:3.7

WORKDIR /usr/src/app/

COPY data_sources/fake/requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY core/ core/
COPY data_sources/fake/ ./

CMD ["python", "cli.py", "crawl"]
