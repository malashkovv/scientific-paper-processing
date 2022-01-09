import typer

from crawler import start_crawling

app = typer.Typer()


@app.command()
def crawl():
    start_crawling()


if __name__ == "__main__":
    app()
