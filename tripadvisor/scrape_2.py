# from scrape_1 import init_webdriver
from time import sleep
from datetime import datetime as dt
from bs4 import BeautifulSoup
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait

BASE = "http://www.tripadvisor.nl"
XPATH_NEXT_BUTTON = "//a[@class='nav next rndBtn ui_button primary taLnk']"
XPATH_LISTING_TITLE = "//div[@class='listing_title']/a"
XPATH_LISTING_TITLE_SPACE = "//div[@class='listing_title ']/a"
XPATH_BUTTON_DISABLED = "//span[@class='nav next disabled']"
SLEEP = 0.25


def wait_for(driver, elem):
    try:
        element_present = ec.presence_of_element_located((By.XPATH, elem))
        WebDriverWait(driver, 5).until(element_present)
    except TimeoutException:
        print("Timed out waiting for page to load")


def provincie(url_str):
    if "North_Holland" in url_str:
        return "Noord-Holland"
    if "Flevoland" in url_str:
        return "Flevoland"


def scroll_down(drv):
    last_height = drv.execute_script("return document.body.scrollHeight")

    while True:
        drv.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(SLEEP)

        new_height = drv.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def strip_link(js_link: str) -> str:
    import re
    return re.search(r"[^*.]?(/Attraction[\w+-]+.html)", js_link,
                     re.IGNORECASE).group(1)


def result_1(soup, link):
    return {
        (
            i.find("a").text,
            strip_link(i.find("a")["href"]),
            dt.now(),
            "NEW",
            provincie(link),
            link
        )
        for i in soup.find_all('div', {'class': 'listing_title'})
    }


def result_2(soup, link):
    return {
        (
            i.find("a").text,
            strip_link(i.find("a")["href"]),
            dt.now(),
            "NEW",
            provincie(link),
            link
        )
        for i in soup.find_all('div', {'class': 'listing_title '})
    }


def get_data(link: str, driver):

    driver.get(BASE + link)

    page_counter = 1
    result = set()

    while True:
        page_counter += 1

        scroll_down(driver)

        soup = BeautifulSoup(driver.page_source, features='lxml')
        result1 = result_1(soup, link)
        result2 = result_2(soup, link)

        for i in result1.union(result2):
            result.add(i)

        if not driver.find_elements_by_xpath(XPATH_BUTTON_DISABLED):

            if driver.find_elements_by_xpath(XPATH_NEXT_BUTTON):
                wait_for(driver, XPATH_NEXT_BUTTON)
                driver.find_element_by_xpath(XPATH_NEXT_BUTTON).click()

                print("CLICK...   (pagina {})".format(page_counter))
            else:
                break
        else:
            break
        print(result)

    return list(result)
