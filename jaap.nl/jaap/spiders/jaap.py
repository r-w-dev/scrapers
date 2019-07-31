# -*- coding: utf-8 -*-
import scrapy
from datetime import datetime
from items import JaapLoader


class JaapSpider(scrapy.Spider):

    name = 'jaap'
    allowed_domains = ['jaap.nl']

    def __init__(self, start_url_list, *args, **kwargs):
        super(JaapSpider, self).__init__(*args, **kwargs)
        self.start_urls = ['https://www.jaap.nl/koophuizen/' + i for i in start_url_list]

    def parse(self, response):
        # get list of links from search results
        search_result = response.xpath('//a[@class="property-inner"]/@href').extract()

        # follow link in search results and yield item (house) from datapage
        for link in search_result:
            yield response.follow(link, callback=self.parse_features)

        # goto next page in search until no more pages
        next_page = response.xpath('//a[@rel="next"]/@href').extract_first()
        if next_page != '#':
            yield response.follow(next_page, callback=self.parse)

    def parse_features(self, response):
        # replace '<br>' tags with ', ': makes multiple value parsing easier ('bijzonderheden' field)
        response = response.replace(body=response.body.replace(b'<br />', b', '))

        # initialize itemLoader
        loader = JaapLoader(response=response)

        # xpaths
        feature_xpath = \
            '//div[@class="detail-tab-content kenmerken"]//td[@class="value"]/text()'
        price_xpath = \
            '//div[@class="detail-tab-content woningwaarde"]/table[1]//td[@class="value"]/text()'
        price2_xpath = \
            '//div[@class="detail-tab-content woningwaarde"]//td[@class="value-3-3"]/text()'
        date_price_changes_xpath = \
            '//div[@class="detail-tab-content woningwaarde"]/table[2]//div[@class="no-dots"]/text()'
        value_price_changes_xpath = \
            '//div[@class="detail-tab-content woningwaarde"]//td[@class="value-1-2"]/text()'
        status_xpath = \
            '//div[@class="main-photo"]/div/div/span/text()'
        status2_xpath = \
            '//div[@class="detail-address"]/span/text()'

        # load list of url data to add to item
        url_list = response.request.url.split('/')

        # yield item loaded with data, extracted from the website
        loader.add_xpath('soort_woning',                 feature_xpath)
        loader.add_xpath('bouwjaar',                     feature_xpath)
        loader.add_xpath('oppervlakte',                  feature_xpath)
        loader.add_xpath('inhoud',                       feature_xpath)
        loader.add_xpath('perceeloppervlakte',           feature_xpath)
        loader.add_xpath('bijzonderheden',               feature_xpath)
        loader.add_xpath('isolatie',                     feature_xpath)
        loader.add_xpath('verwarming',                   feature_xpath)
        loader.add_xpath('energielabel',                 feature_xpath)
        loader.add_xpath('energieverbruik',              feature_xpath)
        loader.add_xpath('staat_onderhoud_binnen',       feature_xpath)
        loader.add_xpath('aantal_kamers',                feature_xpath)
        loader.add_xpath('aantal_slaapkamers',           feature_xpath)
        loader.add_xpath('sanitaire_voorzieningen',      feature_xpath)
        loader.add_xpath('keuken',                       feature_xpath)
        loader.add_xpath('staat_onderhoud_buiten',       feature_xpath)
        loader.add_xpath('staat_schilderwerk',           feature_xpath)
        loader.add_xpath('tuin',                         feature_xpath)
        loader.add_xpath('uitzicht',                     feature_xpath)
        loader.add_xpath('balkon',                       feature_xpath)
        loader.add_xpath('garage',                       feature_xpath)
        loader.add_xpath('aantal_keer_getoond',          feature_xpath)
        loader.add_xpath('aantal_keer_getoond_gisteren', feature_xpath)

        loader.add_xpath('aangeboden_sinds',             price_xpath)
        loader.add_xpath('huidige_vraagprijs',           price_xpath)
        loader.add_xpath('oorspr_vraagprijs',            price_xpath)

        loader.add_xpath('prijs_per_m2',                 price2_xpath)
        loader.add_xpath('tijd_in_de_verkoop',           price2_xpath)

        loader.add_xpath('prijs_wijzigingen_data',       date_price_changes_xpath)
        loader.add_xpath('prijs_wijzigingen_data',       value_price_changes_xpath)

        loader.add_xpath('status',                       status_xpath)
        loader.add_xpath('status',                       status2_xpath)

        loader.add_value('plaats',                       url_list[6])
        loader.add_value('postcode',                     url_list[7], re='^[\d]{4}[a-zA-Z]{2}$')
        loader.add_value('straat',                       url_list[8], re='^[\d|\']*[\'a-zA-Z\.\+\-]+')
        loader.add_value('huisnummer',                   url_list[8], re='[^\de]\d{1,5}')
        loader.add_value('huisletter',                   url_list[8])
        loader.add_value('toevoeging',                   url_list[8])
        loader.add_value('provincie',                    url_list[4])
        loader.add_value('bron',                         url_list[2])
        loader.add_value('jaap_id',                      url_list[9])
        loader.add_value('laatste_scrape',               datetime.now())

        return loader.load_item()
