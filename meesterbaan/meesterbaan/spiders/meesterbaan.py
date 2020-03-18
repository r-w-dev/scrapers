import logging
from datetime import datetime as dt

import scrapy

from meesterbaan.items import Vacature, VacatureLoader


def _xpath(
        field: str,
        field_class: str = 'id',
        tag: str = 'span',
        href: bool = False,
        norm: bool = True
) -> str:
    text_href = '@href' if href else 'text()'
    base = f"//{tag}[contains(@{field_class}, '{field}')]/{text_href}"
    return f'normalize-space({base})' if norm else base


class Meesterbaan(scrapy.Spider):

    name = 'meesterbaan'
    now = dt.now().strftime("%Y%m%d_%H%M")
    output_file = f'output_{now}.json'
    custom_settings = {
        'FEED_URI': output_file,
        'FEED_EXPORT_ENCODING': 'windows-1252',
        'FEED_FORMAT': 'json',
        'LOG_LEVEL': logging.INFO
    }

    base_url = 'https://www.meesterbaan.nl/onderwijs/vacatures.aspx?id_sector=-1&id_regio=-1&id_functie=-1'

    sets_five = 0
    current_page_counter = 0

    def start_requests(self):
        yield scrapy.FormRequest(self.base_url, callback=self._parse_search)

    def _parse_search(self, response):
        links = response.xpath("//a[contains(@id, 'hplLeesMeer')]/@href").getall()
        for link in links:
            yield scrapy.Request(link, callback=self.parse)

        cur_page = response.xpath("//span[@class='CurrentPage']/text()").get()
        cur_page = int(cur_page) if cur_page is not None else -1

        if cur_page != self.current_page_counter and cur_page != -1:
            # set counter == cur_page value
            self.current_page_counter += 1

            # calculate next number link
            next_number = cur_page - (4 * self.sets_five)

            # subtract the number of sets_five -1 if were past the first set
            if self.sets_five > 1:
                next_number -= (self.sets_five-1)

            if cur_page % 5 == 0:
                self.sets_five += 1
                print("update sets_five: ", self.sets_five)

            yield scrapy.FormRequest.from_response(
                response,
                formdata={
                    'ctl00$plhControl$DataPager1$ctl00$ctl0{0}'.format(next_number): ''
                },
                callback=self._parse_search
            )

    def parse(self, response):
        vac = VacatureLoader(item=Vacature(), response=response)

        vac.add_xpath('naam_vacature', _xpath('ctl00_plhControl_lblTitel'))
        vac.add_xpath('naam_school', _xpath('ctl00_plhControl_lblSchool', tag='a'))
        vac.add_xpath('plaats', _xpath('ctl00_plhControl_lblPlaats'))
        vac.add_xpath('sector', _xpath('ctl00_plhControl_lblSector'))
        vac.add_xpath('denominatie', _xpath('ctl00_plhControl_lblDenominatie'))
        vac.add_xpath('dienstverband', _xpath('ctl00_plhControl_lblDienstverband'))
        vac.add_xpath('functie_titel', _xpath('ctl00_plhControl_lblFunctie'))
        vac.add_xpath('fte', _xpath('ctl00_plhControl_lblWTF'))
        vac.add_xpath('opleiding', _xpath('ctl00_plhControl_txtBevoegdheden'))
        vac.add_xpath('salaris_schaal', _xpath('ctl00_plhControl_lblSalarisSchalen'))
        vac.add_xpath('plaatsings_datum', _xpath('ctl00_plhControl_lblPlaatsing2'))
        vac.add_xpath('website', _xpath('ctl00_plhControl_hplWebsite', tag='a'))

        yield vac.load_item()
