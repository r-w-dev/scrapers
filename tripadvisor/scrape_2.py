#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape links naar attracties.

@author: Roel de Vries
@email: roel.de.vries@amsterdam.nl
"""
from datetime import datetime as dt
from time import sleep

from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

BASE = 'http://www.tripadvisor.com'
XPATH_NEXT_BUTTON = "//a[@class='nav next rndBtn ui_button primary taLnk']"
XPATH_LISTING_TITLE = "//div[@class='listing_title']/a"
XPATH_LISTING_TITLE_SPACE = "//div[@class='listing_title ']/a"
XPATH_BUTTON_DISABLED = "//span[@class='nav next disabled']"
SLEEP = 0.25


def _wait_for(driver, elem: str):
    try:
        element_present = ec.presence_of_element_located((By.XPATH, elem))
        WebDriverWait(driver, 2).until(element_present)
    except TimeoutException:
        print('Timed out waiting for page to load')


def _provincie(url_str: str) -> str:
    if 'North_Holland' in url_str:
        return 'Noord-Holland'
    if 'Flevoland' in url_str:
        return 'Flevoland'
    else:
        return ''


def _scroll_down(drv):
    last_height = drv.execute_script('return document.body.scrollHeight')

    while True:
        drv.execute_script('window.scrollTo(0, document.body.scrollHeight);')
        sleep(SLEEP)

        new_height = drv.execute_script('return document.body.scrollHeight')
        if new_height == last_height:
            break
        last_height = new_height


def _strip_link(js_link: str) -> str:
    import re
    return re.search(r'[^*.]?(/Attraction[\w+-]+.html)', js_link,
                     re.IGNORECASE).group(1)


def _find_link(bs_obj) -> str:
    if 'href' in bs_obj.attrs:
        return _strip_link(bs_obj['href'])
    elif 'onclick' in bs_obj.attrs:
        return _strip_link(bs_obj['onclick'])
    else:
        print(bs_obj)
        return "GEEN LINK"


def _get_links(soup: BeautifulSoup, link: str) -> set:
    return {
        (
            i.find('a').text if i.find('a') is not None else 'GEEN TITEL',
            _find_link(i.find('a')),
            dt.now().date(),
            'NEW',
            _provincie(link),
            link
        )
        for i in soup.find_all('div', {'class': ['listing_title', 'listing_title ']})
    }


def get_data2(link: str, driver) -> list:
    """Return list met attractie links."""
    driver.get(BASE + link)

    page_counter = 1
    result = set()

    while True:
        page_counter += 1

        _scroll_down(driver)

        soup = BeautifulSoup(driver.page_source, features='lxml')
        data = _get_links(soup, link)

        for i in data:
            print(i[1])
            result.add(i)

        if not driver.find_elements_by_xpath(XPATH_BUTTON_DISABLED):

            if driver.find_elements_by_xpath(XPATH_NEXT_BUTTON):
                _wait_for(driver, XPATH_NEXT_BUTTON)

                driver.find_element_by_xpath(XPATH_NEXT_BUTTON).click()

                print('CLICK...   (pagina {0})'.format(page_counter))
            else:
                break
        else:
            break

    return list(result)
