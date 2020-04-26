"""
Scrape links naar attracties.

@author: Roel de Vries
@email: roel.de.vries@amsterdam.nl
"""
import re
from datetime import datetime as dt
from time import sleep

import bs4
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from tripadvisor.browser import Browser, scroll_down, wait_for_document_ready_state

BASE = 'http://www.tripadvisor.com'

XPATH_NEXT_BUTTON_1 = "//a[@class='nav next rndBtn ui_button primary taLnk']"
XPATH_NEXT_BUTTON_2 = "//a[@class='ui_button nav next primary ']"

XPATH_BUTTON_DISABLED = "//span[@class='ui_button nav next primary disabled']"

XPATH_LISTING_TITLE = "//div[@class='listing_title']/a"
XPATH_LISTING_TITLE_SPACE = "//div[@class='listing_title ']/a"
# XPATH_BUTTON_DISABLED = "//span[@class='nav next disabled']"

LOC_CATEGORY_ITEM = 'attractions-attraction-filtered-main-index__listItem--3trCl'


def _wait_for(driver, elem: str):
    try:
        element_present = ec.presence_of_element_located((By.XPATH, elem))
        WebDriverWait(driver, 1).until(element_present)
    except TimeoutException:
        print('Timed out waiting for page to load')


def get_provincie_from_url(url_str: str) -> str:
    if 'North_Holland' in url_str:
        return 'Noord-Holland'
    elif 'Flevoland' in url_str:
        return 'Flevoland'
    else:
        return ''


def strip_link(js_link: str) -> str:
    return re.search(r'[^*.]?(/Attraction[\w+-]+.html)', js_link, re.IGNORECASE).group(1)


def find_link(bs_obj: bs4.ResultSet) -> str:
    if not hasattr(bs_obj, 'attrs'):
        print('bs_obj betaat niet.')

    if 'href' in bs_obj.attrs:
        return strip_link(bs_obj.get('href'))

    elif 'onclick' in bs_obj.attrs:
        return strip_link(bs_obj.get('onclick'))

    else:
        print(bs_obj)
        return "GEEN LINK"


def get_links(soup: bs4.BeautifulSoup, link: str) -> list:
    # list_attrs = soup.find(
    #     'div', {'class':
    #         [
    #             'listing_title',
    #             'listing_title ',
    #             re.compile(r'[\w+]listingsContainer')
    #         ]
    #     }
    # )
    # if not list_attrs:
    #     print(f'WARNING: Attracties niet opgehaald ({link})')

    return [
        (
            i.find('a').get_text(strip=True) if i.find('a') is not None else 'GEEN TITEL',
            find_link(i.find('a')),  # link to attractie
            dt.now().date(),
            'NEW',
            get_provincie_from_url(link),
            link
        )
        for i in soup.find_all('div', {'class': [
                lambda x: str(x).startswith('attractions-ap-product-card-ProductCard__productCard'),
                'attraction_element'
            ]
        })
    ]


def get_activities(category: tuple, browser: Browser) -> tuple:
    """Return list met attractie links."""
    driver = browser.driver
    link = category[1]

    driver.get(BASE + link)

    page_counter = 1

    while True:
        page_counter += 1

        scroll_down(browser)

        sleep(0.5)
        soup = bs4.BeautifulSoup(driver.page_source, features='lxml')

        data = get_links(soup, link)

        for i in data:
            yield i

        button_disabled = driver.find_elements_by_xpath(XPATH_BUTTON_DISABLED)
        next_button_enabled1 = driver.find_elements_by_xpath(XPATH_NEXT_BUTTON_1)
        next_button_enabled2 = driver.find_elements_by_xpath(XPATH_NEXT_BUTTON_2)

        if next_button_enabled1:
            next_button = XPATH_NEXT_BUTTON_1
        elif next_button_enabled2:
            next_button = XPATH_NEXT_BUTTON_2
        else:
            next_button = None
            if button_disabled:
                print("Einde categorie.")

        if not button_disabled and (next_button_enabled1 or next_button_enabled2):
            _wait_for(driver, next_button)
            driver.find_element_by_xpath(next_button).click()
            sleep(1)

            print(f'CLICK...   (pagina {page_counter})')

        else:
            break
