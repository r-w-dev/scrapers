# -*- coding: utf-8 -*-

import logging
import os

import scrapy
from pandas import Timestamp

from ...booking.items import Hotel

checkin_date = Timestamp(2019, 8, 26)
checkout_date = Timestamp(2019, 8, 31)
now = Timestamp.now()
location = 'Amsterdam'


class BookingSpider(scrapy.Spider):
    """Booking scraper class."""

    name = 'booking'
    allowed_domains = ['booking.com']

    base_url = 'https://www.booking.com/index.nl.html'
    page_counter = 0

    output_file = 'output_{0}.csv'.format(now.strftime('%Y%m%d %H%M'))
    custom_settings = {
        'FEED_URI': output_file,
        'LOG_LEVEL': logging.INFO
    }

    if os.path.exists(output_file):
        print('##### REMOVING {0} #####'.format(output_file))
        os.remove(output_file)

    def start_requests(self):
        yield scrapy.Request(self.base_url, self._form_request)

    def _form_request(self, response):
        form_data = self._get_form_dict(response)
        form_data['ss'] = 'Amsterdam'
        form_data['checkin_year'] = str(checkin_date.year)
        form_data['checkin_month'] = str(checkin_date.month)
        form_data['checkin_monthday'] = str(checkin_date.day)
        form_data['checkout_year'] = str(checkout_date.year)
        form_data['checkout_month'] = str(checkout_date.month)
        form_data['checkout_monthday'] = str(checkout_date.day)
        form_data['lang'] = 'nl'
        form_data['selected_currency'] = 'EUR'
        form_data['recaptcha_enabled'] = '0'

        yield scrapy.FormRequest.from_response(
            response,
            formid='frm',
            formdata=form_data,
            callback=self._parse_search,
        )

    def _parse_search(self, response):
        """Loop through search results."""
        hotels = response.xpath("//a[@class='hotel_name_link url']/@href").getall()
        if hotels:
            for hotel in hotels:
                yield scrapy.Request(response.urljoin(hotel.strip()),
                                     callback=self.parse)

        next_page = response.xpath("//a[@title='Volgende pagina']/@href").get()

        if next_page is not None:
            self.page_counter += 1
            print('PAGINA {0}'.format(self.page_counter))
            self._get_form_dict(response, echo=False)

            yield scrapy.Request(response.urljoin(next_page.strip()),
                                 callback=self._parse_search)

    def parse(self, resp):
        """Parse hotel data."""

        # inspect_response(resp, self)

        yield Hotel(
            url=resp.url,
            checkin=str(checkin_date.date()),
            checkout=str(checkout_date.date()),
            region=location,

            name=''.join(
                [i.strip() for i in
                 resp.xpath(
                    "//h2[@id='hp_hotel_name']"
                    "/text()"
                ).getall()]),

            address=resp.xpath(
                "normalize-space("
                "//span[contains(@class, 'hp_address')]"
                "/text())"
            ).get(),

            coords=resp.xpath(
                "normalize-space("
                "//a[contains(@class, 'map_static_zoom')]"
                "/@data-atlas-latlng)"
            ).get(),

            id=resp.xpath(
                "normalize-space("
                "//input[contains(@name, 'hotel_id')]"
                "/@value)"
            ).get(),

            rapport_cijfer=resp.xpath(
                "normalize-space("
                "//div[contains(@class, 'hp_review_score_entry')]"
                "//div[@class='bui-review-score__badge']"
                "/text())"
            ).get().replace(',', '.'),

            type_accomodation=resp.xpath(
                "normalize-space("
                "//span[contains(@class, 'hp__hotel-type-badge')]"
                "/text())"
            ).get(),

            booking_rating=resp.xpath(
                "normalize-space("
                "//i[contains(., 'sterren')]"
                "/@title)"
            ).get(),

            full=('full'
                  if resp.xpath("//h3[contains(@class, 'full_hotel')]").getall()
                  else 'available'),

            prijzen=[[
                # max personen
                k.xpath("normalize-space("
                        ".//span[contains(@class, 'bui-u-sr-only')]"
                        "/text())"
                        ).get().replace('Max. personen:', ''),
                # old_rate
                ''.join(
                    self._strip(i) for i in k.xpath(
                        ".//div[contains(@class, 'bui-price-display__original')]"
                        "/text()"
                    ).getall()),
                # new_rate
                ''.join(
                    self._strip(i) for i in k.xpath(
                        ".//div[contains(@class, 'bui-price-display__value')]"
                        "/span"
                        "|"
                        ".//span[@class='important_text']"
                        "/div"
                    ).xpath("./text()").getall())
            ] for k in resp.xpath(
                "//table[@id='hprt-table']"
                "/tbody"
                "/tr")
                if 'hprt-cheapest-block-row' not in k.get()]
        )

    @staticmethod
    def _get_form_dict(resp: scrapy.http.Response, echo=False) -> dict:
        from pprint import pprint
        f = resp.xpath("//form[@id='frm']//input[@type='hidden']")
        res = {k: v for k, v in
               zip(f.xpath("@name").getall(), f.xpath("@value").getall())}
        if echo:
            pprint(res)
        return res

    @staticmethod
    def _strip(line: str) -> str:
        return line \
            .strip() \
            .replace(u'\xa0', '') \
            .replace('.', '') \
            .replace(',', '.') \
            if line is not None else ''
