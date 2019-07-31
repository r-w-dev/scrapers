#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""

@author: Roel de Vries
@email: roel.de.vries@amsterdam.nl
"""
import os

from selenium.webdriver import Firefox, FirefoxOptions, FirefoxProfile
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import scrape_1
import scrape_2
import scrape_3
from Bases import Attractie, Attribute, Categorie
from psql import Psql

os.environ['MOZ_HEADLESS_WIDTH'] = '1920'
os.environ['MOZ_HEADLESS_HEIGHT'] = '1080'


class Browser:
    PM_PATH = "X:\\documents\\toerisme_scraper\\scripts\\driver\\palemoon\\Bin" \
              "\\Palemoon\\palemoon.exe"
    FF_PATH = os.getcwd() + '\\driver\\geckodriver.exe'
    UBLOCK = os.getcwd() + '\\driver\\ublock.xpi'
    headless = False
    block_img = False

    def __init__(self, headless=True, block_img=False):
        self.headless = headless
        self.block_img = block_img

    def __enter__(self):
        self.driver = self._init_webdriver(self.headless, self.block_img)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.close()
        self.driver.quit()
        print('Browser closed...')

    def _init_webdriver(self, headless: bool = True, block_img: bool = False) -> \
            Firefox:
        fp = FirefoxProfile()
        fp.set_preference('http.response.timeout', 10)
        fp.set_preference('dom.max_script_run_time', 10)
        fp.set_preference('gfx.downloadable_fonts.enabled', False)
        fp.set_preference('toolkit.cosmeticAnimations.enabled', False)
        fp.set_preference('browser.sessionhistory.max_total_viewers', 1)
        fp.set_preference('browser.sessionhistory.max_entries', 1)
        fp.set_preference('extensions.checkCompatibility', False)
        fp.set_preference('browser.sessionstore.interval', 60000)
        fp.set_preference('layout.spellcheckDefault', 0)
        fp.set_preference('config.trim_on_minimize', True)
        fp.set_preference('browser.cache.disk.capacity', 100000)
        fp.set_preference('security.dialog_enable_delay', 0)
        if block_img:
            fp.set_preference('permissions.default.image', 2)

        options = FirefoxOptions()
        if headless:
            options.add_argument('-headless')
        options.log.level = 'fatal'

        ff = Firefox(
            firefox_profile=fp,
            executable_path=self.FF_PATH,
            options=options,
        )
        ff.install_addon(self.UBLOCK, temporary=True)
        ff.maximize_window()

        return ff

    def restart(self):
        self.driver.close()
        print("Restarting browser...")
        self.__enter__()
        return self


def get_links_from_list(scrape_list: list, idx: int) -> list:
    return [s[idx] for s in scrape_list]


if __name__ == '__main__':

    with Browser(headless=True, block_img=True) as fox:
        scrape1 = scrape_1.get_data(fox.driver)

        print("Scraped {} categorie links...".format(len(scrape1)))

    links2 = get_links_from_list(scrape1, 2)

    with Browser(headless=True, block_img=True) as fox2:
        for i, link in enumerate(links2):
            scrape2 = scrape_2.get_data(link, fox2.driver)

            print("[{}/{}] {}".format(i + 1, len(links2), link))
            if i % 20 == 0 and i != 0 and i != 1:
                fox2.restart()

        print("Scraped {} attraction links...".format(len(scrape2)))

    links3 = get_links_from_list(scrape2, 1)

    with Browser(headless=False) as fox3:
        scrape3 = []

        for i, link in enumerate(links3):
            scrape3.append(scrape_3.get_data(link, fox3.driver))

            print("[{}/{}] {}".format(i + 1, len(links3), link))
            if i % 20 == 0 and i != 0 and i != 1:
                fox3.restart()

        print("Scraped {} attractions".format(len(scrape3)))

    sessie = Psql().set_sess_maker().sessie_maker()

    try:
        sessie.add_all([Categorie(*c) for c in scrape1])
        sessie.add_all([Attribute(*a) for a in scrape2])
        sessie.add_all([Attractie(*a) for a in scrape3])
        sessie.commit()

    except Exception as ex:
        print(ex.__class__)
        sessie.rollback()
        raise

    finally:
        sessie.close()
