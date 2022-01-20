import typer

from config import settings
from model import train

app = typer.Typer()


@app.command()
def start():
    train(settings)


if __name__ == "__main__":
    app()
