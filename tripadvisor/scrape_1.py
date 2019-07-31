#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape categorieën.

@author: Roel de Vries
@email: roel.de.vries@amsterdam.nl
"""
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

URL = 'https://www.tripadvisor.com'
URL_NH = URL + '/Attractions-g188587-Activities-North_Holland_Province.html'
URL_FL = URL + '/Attractions-g188559-Activities-Flevoland_Province.html'


def _wait_for(driver, xpath_elem: str):
    try:
        element_present = ec.presence_of_element_located((By.XPATH, xpath_elem))
        WebDriverWait(driver, 5).until(element_present)
    except TimeoutException:
        print('Timed out waiting for page to load')


def _get_data_from_items(items: BeautifulSoup, provincie: str = '') -> set:
    from datetime import datetime as dt
    import re

    result = set()

    for i in items:
        result.add(
            (
                re.sub(r'[^a-zA-Z &]', '', i.text).strip(),
                i['href'],
                dt.now().date(),
                'NEW',
                provincie,
            )
        )
    return result


def _get_categories(driver, url: str, provincie: str) -> set:
    driver.get(url)

    driver.find_element_by_xpath(
        "//span[@class='attractions-attraction-overview-main-Pill__pill--3DtDw "
        "attractions-attraction-overview-main-PillShelf__showToggle--nl7jI']"
    ).click()
    _wait_for(driver,
              "//a[@class='attractions-attraction-overview-main-Pill__pill--3DtDw']")
    soup = BeautifulSoup(driver.page_source, features='lxml')

    items = soup.find_all(
        'a', {'class': 'attractions-attraction-overview-main-Pill__pill--3DtDw'}
    )
    return _get_data_from_items(items, provincie)


def get_data(driver) -> list:
    """Return categorieën lijst."""
    nh = _get_categories(driver, URL_NH, 'Noord-Holland')
    fl = _get_categories(driver, URL_FL, 'Flevoland')
    # return fl
    return list(nh.union(fl))
