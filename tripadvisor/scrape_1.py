#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Roel de Vries
@email: roel.de.vries@amsterdam.nl
"""
from bs4 import BeautifulSoup
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait

URL = "https://www.tripadvisor.nl"
URL_NH = URL + "/Attractions-g188587-Activities-North_Holland_Province.html"
URL_FL = URL + "/Attractions-g188559-Activities-Flevoland_Province.html"
SLEEP = 0.1


def wait_for(driver, elem):
    try:
        element_present = ec.presence_of_element_located((By.XPATH, elem))
        WebDriverWait(driver, 5).until(element_present)
    except TimeoutException:
        print("Timed out waiting for page to load")


def get_data_from_items(items: BeautifulSoup, provincie: str = "") -> set:
    from datetime import datetime as dt
    import re

    result = set()

    for i in items:
        result.add(
            (
                re.sub(r"[^a-zA-Z &]", "", i.text).strip(),
                int(re.sub(r"[^0-9]", "", i.text)),
                i["href"],
                dt.now(),
                "NEW",
                provincie,
            )
        )
    return result


def get_categories(driver, url: str, provincie: str) -> set:
    driver.get(url)

    driver.find_element_by_xpath(
        "//span[@class='attractions-attraction-overview-main-Pill__pill--3DtDw "
        "attractions-attraction-overview-main-PillShelf__showToggle--nl7jI']"
    ).click()
    wait_for(driver, "attractions-attraction-overview-main-Pill__pill--3DtDw")
    soup = BeautifulSoup(driver.page_source, features="lxml")

    items = soup.find_all(
        "a", {"class": "attractions-attraction-overview-main-Pill__pill--3DtDw"}
    )
    return get_data_from_items(items, provincie)


def get_data(driver) -> list:
    # nh = get_categories(driver, URL_NH, "Noord-Holland")
    nh = set()
    fl = get_categories(driver, URL_FL, "Flevoland")

    return list(nh.union(fl))
