import re

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Identity


def clean_str(line: str) -> str:
    if not line:
        return ""
    if 'vacature' in line.lower():
        line = line.lstrip('Vacature').lstrip('vacature')

    line = re.sub(r' in [\w -]+', "", line)
    return line.strip().capitalize()


class VacatureLoader(ItemLoader):
    default_input_processor = Identity()
    default_output_processor = TakeFirst()


class Vacature(scrapy.Item):
    naam_vacature = scrapy.Field(input_processor=MapCompose(clean_str))
    naam_school = scrapy.Field()
    plaats = scrapy.Field(input_processor=MapCompose(lambda x: x.capitalize()))
    sector = scrapy.Field()
    denominatie = scrapy.Field()
    dienstverband = scrapy.Field()
    functie_titel = scrapy.Field()
    fte = scrapy.Field()
    opleiding = scrapy.Field()
    salaris_schaal = scrapy.Field()
    plaatsings_datum = scrapy.Field()
    website = scrapy.Field()
