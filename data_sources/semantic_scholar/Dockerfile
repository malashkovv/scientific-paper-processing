FROM python:3.7

WORKDIR /usr/src/app/

COPY data_sources/semantic_scholar/requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY core/ core/
COPY data_sources/semantic_scholar/ ./

CMD ["python", "cli.py", "crawl"]
