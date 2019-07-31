from bs4 import BeautifulSoup
import re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from urllib.parse import parse_qs


BASE = "https://www.tripadvisor.nl"


def wait_for(driver, elem):
    try:
        element_present = ec.presence_of_element_located((By.XPATH, elem))
        WebDriverWait(driver, 7).until(element_present)
    except TimeoutException:
        print("Timed out waiting for page to load")


def extract_phonenr(line):
    return re.sub(r"[^+0-9]", "", line)


def extract_nr(line):
    return int(re.sub(r"[^0-9]", "", line))


def get_data(link: str, driver):

    driver.get(BASE + link)

    wait_for(driver, "//img[@class='mapImg']")

    soup = BeautifulSoup(driver.page_source, features="lxml")

    ta_id = soup.find("div", {"class": "blRow"})
    ta_id = ta_id["data-locid"] if "data-locid" in ta_id else 0

    rating = soup.find("div", {"class": "section rating"})
    rating = rating.find('a')['alt'] if rating is not None else ""

    adres_str = soup.find("span", {"class": "street-address"})
    adres_str = adres_str.text.strip(",") if adres_str is not None else ""

    adres_local = soup.find("span", {"class": "locality"})
    adres_local = adres_local.text.strip(",") if adres_local is not None else ""

    adres_country = soup.find("span", {"class": "country-name"})
    adres_country = adres_country.text if adres_country is not None else ""

    aantal_reviews = soup.find("a", {"class": "seeAllReviews"})
    aantal_reviews = aantal_reviews.text if aantal_reviews is not None else -100

    reviews = soup.find('div', {'class': 'section histogram'})
    reviews = reviews.findall(
        "span", {"class": "row_count row_cell"}
    ) if reviews is not None else []

    if reviews:
        if len(reviews) == 5:
            reviews = [r.text for r in reviews]
        else:
            reviews = [-1, -1, -1, -1, -1]
    else:
        reviews = [-1, -1, -1, -1, -1]

    phonenr = soup.find("div", {"class": "detail_section phone"})
    phonenr = phonenr.text if phonenr is not None else ""

    coords = parse_qs(soup.find('img', {'class': 'mapImg'})['src'])
    coords = coords['center'][0].split(',') if 'center' in coords else 0

    return (
        (
            "NEW",
            link,
            int(ta_id),
            rating.strip() if rating != "" else -1,
            adres_str.strip(),
            adres_local.replace(",", "").strip(),
            adres_country.strip(',').strip(),
            extract_nr(aantal_reviews),
            extract_nr(reviews[0]),
            extract_nr(reviews[1]),
            extract_nr(reviews[2]),
            extract_nr(reviews[3]),
            extract_nr(reviews[4]),
            extract_phonenr(phonenr),
            float(coords[0]),
            float(coords[1]),
        )
    )
