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
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from tripadvisor.browser import Browser, hide_elements, scroll_into_view

URL = 'https://www.tripadvisor.com'
URL_NH = f'{URL}/Attractions-g188587-Activities-North_Holland_Province.html'
URL_FL = f'{URL}/Attractions-g188559-Activities-Flevoland_Province.html'

XPATH_VIEW_MORE_BUTTON = "//span[@class='_3S09qsQh _1dTP6k0z _30GXgBoj']"
XPATH_VIEW_MORE_BUTTON_NL = ("Alles weergeven", "Minder weergeven")
XPATH_VIEW_MORE_BUTTON_EN = ("See all", "See fewer")

CATEGORY_LINK_CLASS = "_3S09qsQh _30GXgBoj"


def get_categories_prov(browser: Browser) -> bs4.ResultSet:
    try:
        scroll_into_view((By.XPATH, XPATH_VIEW_MORE_BUTTON), browser)
        hide_elements(["sbx_banner"], browser)

        browser.driver.find_element_by_xpath(XPATH_VIEW_MORE_BUTTON).click()

        element_present = ec.text_to_be_present_in_element(
            locator=(By.XPATH, XPATH_VIEW_MORE_BUTTON),
            text_=XPATH_VIEW_MORE_BUTTON_EN[1]
        )
        WebDriverWait(browser.driver, 2).until(element_present)

    except TimeoutException as t:
        raise t
    except NoSuchElementException as e:
        raise e

    else:
        cat = bs4.BeautifulSoup(
            browser.driver.page_source,
            features='lxml'
        ).find_all('a', {'class': CATEGORY_LINK_CLASS})

    sleep(2)
    return cat


def get_data_from_item(item: bs4.Tag, provincie: str) -> tuple:
    return (
        re.sub(r'[^a-zA-Z &]', '', item.get_text(strip=True)).strip(),
        item['href'],  # link to activities
        dt.now().date().isoformat(),
        'NEW',
        provincie,
    )


def get_categories(browser: Browser) -> list:
    """Return categorieën lijst."""
    browser.get(URL_NH)
    nh = (get_data_from_item(item, 'Noord-Holland') for item in get_categories_prov(browser))

    browser.get(URL_FL)
    fl = (get_data_from_item(item, 'Flevoland') for item in get_categories_prov(browser))

    return [item for item in chain(nh, fl)]

