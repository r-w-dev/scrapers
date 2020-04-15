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

from browser import Browser, scroll_down

BASE = 'http://www.tripadvisor.com'

XPATH_NEXT_BUTTON = "//a[@class='nav next rndBtn ui_button primary taLnk']"
XPATH_LISTING_TITLE = "//div[@class='listing_title']/a"
XPATH_LISTING_TITLE_SPACE = "//div[@class='listing_title ']/a"
XPATH_BUTTON_DISABLED = "//span[@class='nav next disabled']"

SLEEP = 0.2  # minimaal 0.2 met headless=True


def _wait_for(driver, elem: str):
    try:
        element_present = ec.presence_of_element_located((By.XPATH, elem))
        WebDriverWait(driver, 2).until(element_present)
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
    if 'href' in bs_obj.attrs:
        return strip_link(bs_obj.get('href'))

    elif 'onclick' in bs_obj.attrs:
        return strip_link(bs_obj.get('onclick'))

    else:
        print(bs_obj)
        return "GEEN LINK"


def get_links(soup: bs4.BeautifulSoup, link: str) -> list:
    return [
        (
            i.find('a').get_text(strip=True) if i.find('a') is not None else 'GEEN TITEL',
            find_link(i.find('a')),  # link to attractie
            dt.now().date(),
            'NEW',
            get_provincie_from_url(link),
            link
        )
        for i in soup.find_all(
            'div', {'class': [
                'listing_title',
                'listing_title ',
                re.compile(r'[\w+]listingsContainer')]}
        )
    ]


def get_activities(category: tuple, browser: Browser) -> tuple:
    """Return list met attractie links."""
    driver = browser.driver
    link = category[1]

    driver.get(BASE + link)
    sleep(SLEEP)

    page_counter = 0

    while True:
        page_counter += 1

        scroll_down(browser)

        soup = bs4.BeautifulSoup(driver.page_source, features='lxml')
        data = get_links(soup, link)

        for i in data:
            yield i

        if (
                not driver.find_elements_by_xpath(XPATH_BUTTON_DISABLED) and
                driver.find_elements_by_xpath(XPATH_NEXT_BUTTON)
        ):
            _wait_for(driver, XPATH_NEXT_BUTTON)
            driver.find_element_by_xpath(XPATH_NEXT_BUTTON).click()

            print(f'CLICK...   (pagina {page_counter})')
        else:
            break
