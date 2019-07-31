# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Field, Item


class Hotel(Item):
    name = Field()
    address: str = Field()
    coords: str = Field()
    full: str = Field()
    id: str = Field()
    booking_rating: str = Field()
    type_accomodation: str = Field()
    rapport_cijfer: str = Field()
    region: str = Field()
    checkin: str = Field()
    checkout: str = Field()
    # kamer_max_pers: list = Field()
    # kamer_prijzen: list = Field()
    # old_rate: list = Field()
    # cur_rate: list = Field()
    prijzen = Field()
    url: str = Field()
    # room1: Room = Room()
    # room2: Room = Room()
    # room3: Room = Room()
    # room4: Room = Room()
    # loc_index: int = None
