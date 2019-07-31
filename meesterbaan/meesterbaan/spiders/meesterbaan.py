import scrapy
from meesterbaan.items import Vacature
from datetime import datetime as dt
import logging


class Meesterbaan(scrapy.Spider):

    name = 'meesterbaan'
    output_file = 'output_{0}.csv'.format(dt.now().strftime('%Y%m%d %H%M'))
    custom_settings = {
        'FEED_URI': output_file,
        'FEED_EXPORT_ENCODING': 'windows-1252',
        'FEED_FORMAT': 'csv',
        'LOG_LEVEL': logging.INFO
    }

    base_url = 'https://www.meesterbaan.nl/onderwijs/vacatures.aspx' \
               '?id_sector=-1&id_regio=-1&id_functie=-1'

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
        yield Vacature(
            naam_vacature=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_lblTitel')]/text()"
            ).get(),

            naam_school=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_lblSchool')]/text()"
            ).get(),

            plaats=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_lblPlaats')]/text()"
            ).get(),

            sector=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_lblSector')]/text()"
            ).get(),

            denominatie=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_lblDenominatie')]/text()"
            ).get(),

            dienstverband=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_lblDienstverband')]/text()"
            ).get(),

            functie_titel=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_lblFunctie')]/text()"
            ).get(),

            fte=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_lblWTF')]/text()"
            ).get(),

            opleiding=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_txtBevoegdheden')]/text()"
            ).get(),

            salaris_schaal=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_lblSalarisSchalen')]/text()"
            ).get(),

            plaatsings_datum=response.xpath(
                "//span[contains(@id, 'ctl00_plhControl_lblPlaatsing2')]/text()"
            ).get(),
        )
