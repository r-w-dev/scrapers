from scrapy.exceptions import DropItem
import pandas as pd


class DuplicatesPipeline(object):

    def __init__(self):
        self.ids_seen = set()
        self.duplicates = 0

    def process_item(self, item, spider):
        if item['jaap_id'] in self.ids_seen:
            self.duplicates += 1
            raise DropItem("Duplicate item found: %s, %s" % (item['straat'], item['plaats']))
        else:
            self.ids_seen.add(item['jaap_id'])
            return item

    def close_spider(self, spider):
        spider.crawler.stats.set_value('dropped duplicates', self.duplicates)


class UtrechtBS(object):

    def process_item(self, item, spider):
        if 'toevoeging' in item and 'plaats' in item:
            if 'bis' in str(item['toevoeging']) and item['plaats'] == 'Utrecht':
                item['toevoeging'] = 'bs'
        return item


class NAFilter(object):

    def __init__(self):
        self.dropped_huisnummer = 0
        self.dropped_jaap_id = 0
        self.dropped_postcode = 0
        self.columns = [
            'straat', 'huisnummer', 'huisletter', 'toevoeging', 'postcode', 'plaats', 'huidige_vraagprijs',
            'oorspr_vraagprijs', 'prijs_wijzigingen_data', 'prijs_per_m2',
            'aangeboden_sinds', 'tijd_in_de_verkoop', 'status', 'soort_woning', 'bouwjaar', 'oppervlakte',
            'perceeloppervlakte', 'aantal_kamers', 'aantal_slaapkamers', 'inhoud', 'tuin', 'garage', 'energielabel',
            'verwarming', 'isolatie', 'bron', 'laatste_scrape', 'jaap_id']
        self.temp_list = []

    def process_item(self, item, spider):
        if 'huisnummer' not in item:
            self.dropped_huisnummer += 1
            self.temp_list += [dict(item)]
            raise DropItem("Huisnummer is empty:  %s, %s" % (item['straat'], item['plaats']))
        elif 'jaap_id' not in item:
            self.dropped_jaap_id += 1
            self.temp_list += [dict(item)]
            raise DropItem("JaapID is empty:  %s, %s" % (item['straat'], item['plaats']))
        elif 'postcode' not in item:
            self.dropped_postcode += 1
            self.temp_list += [dict(item)]
            raise DropItem("Postcode is empty:  %s, %s" % (item['straat'], item['plaats']))
        else:
            return item

    def close_spider(self, spider):
        scraped_count = spider.crawler.stats.get_value('item_scraped_count')
        if not scraped_count:
            scraped_count = 0
        dropped_count = spider.crawler.stats.get_value('item_dropped_count')
        if not dropped_count:
            dropped_count = 0
        ignored_count = spider.crawler.stats.get_value('httperror/response_ignored_count')
        if not ignored_count:
            ignored_count = 0

        total_ignored = scraped_count + ignored_count + dropped_count
        total_scraped = scraped_count + dropped_count

        spider.crawler.stats.set_value('dropped huisnummer %',
                                       str(round(self.dropped_huisnummer / total_scraped * 100, 2)) + '%')
        spider.crawler.stats.set_value('dropped jaap_id %',
                                       str(round(self.dropped_jaap_id / total_scraped * 100, 2)) + '%')
        spider.crawler.stats.set_value('dropped postcode %',
                                       str(round(self.dropped_postcode / total_scraped * 100, 2)) + '%')

        spider.crawler.stats.set_value('dropped huisnummer', self.dropped_huisnummer)
        spider.crawler.stats.set_value('dropped postcode', self.dropped_postcode)
        spider.crawler.stats.set_value('dropped jaap_id', self.dropped_jaap_id)
        spider.crawler.stats.set_value('dropped response ignored %',
                                       str(round(ignored_count / total_ignored * 100, 2)) + '%')

        pd.DataFrame(self.temp_list, columns=self.columns).to_csv('../../output/dropped_test.csv', index=False)
