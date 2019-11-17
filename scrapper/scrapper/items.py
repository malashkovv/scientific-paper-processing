# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BiorXivPage(scrapy.Item):

    abstract = scrapy.Field()
    category = scrapy.Field()
    doi = scrapy.Field()
    path = scrapy.Field()
    posted = scrapy.Field()
    parsed = scrapy.Field()