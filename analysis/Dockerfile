FROM vmalashkov/apache-spark:spark-3.1.2-h3.2-py-3.8.12

COPY requirements.txt /usr/src/app/requirements.txt

RUN spark-pip install -r /usr/src/app/requirements.txt

COPY . /usr/src/project

RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.24.jar