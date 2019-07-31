import scrapy


class Vacature(scrapy.Item):
    naam_vacature = scrapy.Field()
    naam_school = scrapy.Field()
    plaats = scrapy.Field()
    sector = scrapy.Field()
    denominatie = scrapy.Field()
    dienstverband = scrapy.Field()
    functie_titel = scrapy.Field()
    fte = scrapy.Field()
    opleiding = scrapy.Field()
    salaris_schaal = scrapy.Field()
    plaatsings_datum = scrapy.Field()
