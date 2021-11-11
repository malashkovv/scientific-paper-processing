import typer

from core.log import logger
from crawler import start_crawling

app = typer.Typer()


@app.command()
def crawl():
    start_crawling()


@app.command()
def seed(paper_id: str):
    logger.info(f"Seeding: {paper_id}")


if __name__ == "__main__":
    app()
