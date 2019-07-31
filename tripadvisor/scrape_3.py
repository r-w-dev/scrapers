import re
from decimal import Decimal
from urllib.parse import parse_qs

from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

BASE = 'https://www.tripadvisor.com'


def wait_for(driver, xpath_elem):
    """Wait for element to appear on website."""
    try:
        element_present = ec.presence_of_element_located((By.XPATH, xpath_elem))
        WebDriverWait(driver, 5).until(element_present)
    except TimeoutException:
        print('WARNING: Timed out waiting for page to load')


def extract_phonenr(line):
    """Extract telefoonnummer uit string."""
    return re.sub(r'[^+0-9]', '', line)


def extract_nr(line):
    """Extract alle cijfers uit een string."""
    return int(re.sub(r'[^0-9]', '', line))


def get_data(link: str, driver):
    """Verwerk alle data van website <link>."""
    driver.get(BASE + link)

    wait_for(driver, "//img[@class='mapImg']")

    soup = BeautifulSoup(driver.page_source, features='lxml')

    title = soup.find('h1', {'class': 'ui_header h1'})
    title = title.text if title is not None else ''

    ta_id = re.search(r'^/[\w]+-g([0-9]+)-d([0-9]+)', link, re.IGNORECASE)
    ta_id = int(ta_id.group(2)) if ta_id is not None else 0

    rating = soup.find('div', {'class': 'section rating'})
    rating = Decimal(re.search(r'[0-9.]{1,3}', rating.find('a')['alt']).group(0)) \
        if rating is not None else -1

    adres_str = soup.find('span', {'class': 'street-address'})
    adres_str = adres_str.text.strip(',').strip() if adres_str is not None else ''

    adres_local = soup.find('span', {'class': 'locality'})
    adres_local = adres_local.text.strip(',').replace(',', '').strip() \
        if adres_local is not None else ''

    pcode = re.match(r'[0-9]{4} [A-Z]{2}|[0-9]{4}', adres_local)
    pcode = pcode[0].strip() if pcode is not None else ''

    plaats = re.sub(r'[0-9|A-Z{1}]+[A-Z|0-9]{3}[ ]|[A-Z]{2} ', '', adres_local).strip()
    plaats = '' if len(plaats) <= 2 else plaats

    adres_country = soup.find('span', {'class': 'country-name'})
    adres_country = adres_country.text.strip(',').strip() \
        if adres_country is not None else ''

    aantal_reviews = soup.find('a', {'class': 'seeAllReviews'})
    aantal_reviews = extract_nr(aantal_reviews.text) \
        if aantal_reviews is not None else -100

    reviews = soup.find('div', {'class': 'section histogram'})
    reviews = reviews.find_all(
        'span', {'class': 'row_count row_cell'}
    ) if reviews is not None else []

    if reviews:
        if len(reviews) == 5:
            reviews = [extract_nr(r.text) for r in reviews]
        else:
            reviews = [-1, -1, -1, -1, -1]
    else:
        reviews = [-1, -1, -1, -1, -1]

    phonenr = soup.find('div', {'class': 'detail_section phone'})
    phonenr = extract_phonenr(phonenr.text) if phonenr is not None else ''

    coords = soup.find('img', {'class': 'mapImg'})
    if coords is not None:
        coords = parse_qs(coords['src'])
        coords = coords['center'][0].split(',') if 'center' in coords else [0, 0]
        coords = [Decimal(c) for c in coords]
    else:
        coords = [0, 0]

    return (
        'NEW',
        title,
        link,
        ta_id,
        rating,
        adres_str,
        adres_local,
        pcode,
        plaats,
        adres_country,
        aantal_reviews,
        reviews[0],
        reviews[1],
        reviews[2],
        reviews[3],
        reviews[4],
        phonenr,
        coords[0],
        coords[1],
    )
