import os
import sqlalchemy as sql


def create_reporting_engine():
    login, pwd = os.environ.get('REPORTING_LOGIN', 'login'), os.environ.get('REPORTING_PASSWORD', 'pwd')
    database_name = os.environ.get('REPORTING_DATABASE', 'reporting')
    return sql.create_engine(f"postgresql://{login}:{pwd}@reporting-db:5432/{database_name}")