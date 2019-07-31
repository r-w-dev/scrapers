import scrapy
import datetime


class ZahSpider(scrapy.Spider):

    name = 'zoekallehuizen'
    allowed_domains = ['zah.nl', 'pararius.nl']

    min_price = [25000 + i * 5000 for i in range(195)]
    max_price = [30000 + i * 5000 for i in range(195)]

    interm_urls = ['https://www.zah.nl/te-koop/?minPrice=' + str(x) + '&maxPrice=' + str(y) + '&page=1' for x, y in
                   zip(min_price, max_price)]
    start_end = ['https://www.zah.nl/te-koop/?maxPrice=25000&page=1',
                 'https://www.zah.nl/te-koop/?minPrice=1000000&page=1']

    # start_urls = [start_end[0]] + interm_urls + [start_end[-1]]
    start_urls = ['https://www.zah.nl/te-koop/?minPrice=350000&maxPrice=375000&page=70']
    # start_urls = ['https://www.zah.nl/te-koop/?search=Herveld&rooms=&type=']

    def parse(self, response):
        # get links from search results
        search_result = response.xpath('//div[@class="result"]//a/@href').extract()

        # loop links in search results and yield feature dict from datapage
        for link in search_result:
            yield response.follow(link, callback=self.parse_features)

        # goto next page in search until no more pages
        next_page = response.xpath('//div[@class="pagination"]//ul//a[text()="Volgende "]/@href').extract_first()
        if next_page != '#':
            next_page = 'https://www.zah.nl/' + str(next_page)
            yield response.follow(next_page, callback=self.parse)

    def parse_features(self, response):
        features = response.xpath('//div[@class="details-container"]//dl//text()').extract()
        address = response.xpath('//div[@class="main-features"]//h1//ins[@class="street"]//text()').extract()
        city = response.xpath('//div[@class="main-features"]//h1//ins[@class="city"]//text()').extract()

        address = ['Address'] + [i.replace('\n', '').replace(' ', '') for i in address]
        city = ['City'] + city
        features = [i for i in features if i.strip() != '']
        date = ['Date'] + [str(datetime.date.today())]
        source = ['Source', 'www.zah.nl']

        data = features + address + city + date + source

        if ('Provincie' in data) & (len(data) % 2 != 0):
            return {data[i]: data[i + 1] for i in range(1, len(data), 2)}
        else:
            return {data[i]: data[i + 1] for i in range(0, len(data), 2)}
