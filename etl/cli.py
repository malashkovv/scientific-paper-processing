import json
import datetime as dt

import typer

from core.log import logger
from pipeline import process_stream
from config import settings

app = typer.Typer()


@app.command()
def start():
    process_stream(settings)


if __name__ == "__main__":
    app()
