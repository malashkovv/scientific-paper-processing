import re
import logging
from datetime import datetime
from urllib.parse import urlparse

import scrapy

from scrapper.items import BiorXivPage
from scrapper.settings import BIORXIV_CATEGORIES, BIORXIV_START_PAGE, BIORXIV_END_PAGE

logger = logging.getLogger('biorxiv_spider_logger')


class BiorXivSpider(scrapy.Spider):
    name = "biorxiv"

    domain = "https://biorxiv.org"

    def start_requests(self):
        for category in BIORXIV_CATEGORIES:
            yield scrapy.Request(url=f"{self.domain}/collection/{category}?page={BIORXIV_START_PAGE}",
                                 callback=self.parse_collection, meta={'category': category})

    def parse_collection(self, response):
        category = response.meta['category']
        for url in response.xpath('//a[has-class("highwire-cite-linked-title")]/@href'):
            yield response.follow(f"{self.domain}/{url.get()}", self.parse_content, meta={'category': category})

        current_page = int(urlparse(response.url).query.split('=')[1])
        last_page = BIORXIV_END_PAGE or int(response.xpath('//li[has-class("pager-last")]/a/text()').get())
        logger.info(f"Currently on page {current_page} in {category}. Last page is {last_page}.")
        if current_page >= last_page:
            logging.info(f"Got to the last page {last_page}. Completing {category}")
            return
        yield response.follow(f"{self.domain}/collection/{category}?page={current_page + 1}",
                              self.parse_collection, meta={'category': category})

    def parse_content(self, response):
        item = BiorXivPage()
        raw_abstract = response.xpath('//div[has-class("abstract")]').get()
        item['abstract'] = re.sub("<[^<]+>", " ", raw_abstract)
        item['path'] = urlparse(response.url).path
        raw_date = response.xpath('//div[contains(text(),"Posted")]/text()').get().strip().split('\xa0')[1].strip('.')
        item['posted'] = datetime.strptime(raw_date, "%B %d, %Y").date().isoformat()
        item['doi'] = urlparse(
            response.xpath('//span[has-class("highwire-cite-metadata-doi")]/text()').get().strip()).path
        item['category'] = response.meta['category']
        item['parsed'] = datetime.utcnow().isoformat()
        yield item
