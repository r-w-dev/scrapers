"""
Scrape links naar attracties.

@author: Roel de Vries
@email: roel.de.vries@amsterdam.nl
"""
import re
from datetime import datetime as dt
from time import sleep

from bs4 import BeautifulSoup, ResultSet
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

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


def scroll_down(driver: Chrome):
    # Get scroll height
    last_height = driver.execute_script("return document.body.scrollHeight")
    try_times = 0

    while True:
        # Scroll down to bottom
        driver.execute_script("window.scrollBy(0,2000)")

        # Wait to load page
        sleep(SLEEP)

        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")

        if last_height == new_height:
            try_times += 1

        if try_times > 3:
            try_times = 0
            break
        last_height = new_height


def strip_link(js_link: str) -> str:
    return re.search(r'[^*.]?(/Attraction[\w+-]+.html)', js_link, re.IGNORECASE).group(1)


def find_link(bs_obj: ResultSet) -> str:
    if 'href' in bs_obj.attrs:
        return strip_link(bs_obj['href'])

    elif 'onclick' in bs_obj.attrs:
        return strip_link(bs_obj['onclick'])

    else:
        print(bs_obj)
        return "GEEN LINK"


def get_links(soup: BeautifulSoup, link: str) -> set:
    return {
        (
            i.find('a').text if i.find('a') is not None else 'GEEN TITEL',
            find_link(i.find('a')),
            dt.now().date(),
            'NEW',
            get_provincie_from_url(link),
            link
        )
        for i in soup.find_all('div', {'class': ['listing_title', 'listing_title ']})
    }


def get_data2(link: str, driver: Chrome) -> list:
    """Return list met attractie links."""
    driver.get(BASE + link)
    sleep(SLEEP)

    page_counter = 1
    result = set()

    while True:
        page_counter += 1

        scroll_down(driver)

        soup = BeautifulSoup(driver.page_source, features='lxml')
        data = get_links(soup, link)

        for i in data:
            print(i[1])
            result.add(i)

        if (
                not driver.find_elements_by_xpath(XPATH_BUTTON_DISABLED) and
                driver.find_elements_by_xpath(XPATH_NEXT_BUTTON)
        ):
            _wait_for(driver, XPATH_NEXT_BUTTON)
            driver.find_element_by_xpath(XPATH_NEXT_BUTTON).click()

            print(f'CLICK...   (pagina {page_counter})')
        else:
            break

    return list(result)
