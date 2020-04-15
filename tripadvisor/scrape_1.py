"""
Scrape categorieën.

@author: Roel de Vries
@email: roel.de.vries@amsterdam.nl
"""
import re
from datetime import datetime as dt
from itertools import chain
from time import sleep

import bs4
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from browser import Browser, hide_elements

URL = 'https://www.tripadvisor.com'
URL_NH = f'{URL}/Attractions-g188587-Activities-North_Holland_Province.html'
URL_FL = f'{URL}/Attractions-g188559-Activities-Flevoland_Province.html'

XPATH_VIEW_MORE_BUTTON = "//span[@class='_3S09qsQh _1dTP6k0z _30GXgBoj']"
XPATH_VIEW_MORE_BUTTON_NL = ("Alles weergeven", "Minder weergeven")
XPATH_VIEW_MORE_BUTTON_EN = ("See all", "See fewer")

CATEGORY_LINK_CLASS = "_3S09qsQh _30GXgBoj"


def _get_categories(browser: Browser, url: str) -> bs4.ResultSet:
    driver = browser.driver
    driver.get(url)

    hide_elements(["sbx_banner"], browser)

    driver.find_element_by_xpath(XPATH_VIEW_MORE_BUTTON).click()

    element_present = ec.text_to_be_present_in_element(
        locator=(By.XPATH, XPATH_VIEW_MORE_BUTTON),
        text_=XPATH_VIEW_MORE_BUTTON_EN[1]
    )
    WebDriverWait(driver, 5).until(element_present)
    sleep(0.75)
    return bs4.BeautifulSoup(driver.page_source, features='lxml').find_all('a', {'class': CATEGORY_LINK_CLASS})


def get_data_from_item(item, provincie: str) -> tuple:
    return (
        re.sub(r'[^a-zA-Z &]', '', item.get_text(strip=True)).strip(),
        item['href'],  # link to activities
        dt.now().date(),
        'NEW',
        provincie,
    )


def get_categories(browser: Browser) -> list:
    """Return categorieën lijst."""
    nh = (get_data_from_item(item, 'Noord-Holland') for item in _get_categories(browser, URL_NH))
    fl = (get_data_from_item(item, 'Flevoland') for item in _get_categories(browser, URL_FL))

    return [item for item in chain(nh, fl)]
