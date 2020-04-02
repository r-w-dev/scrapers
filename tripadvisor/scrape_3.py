import json
import re
from time import sleep
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from shapely.geometry import Point

from browser import Browser

URL = 'https://www.tripadvisor.com'


def find_value_nested_dict(key: Any, dct: dict) -> Optional[Any]:
    val1 = dct.get(key, None)
    if val1:
        return val1
    else:
        for k in dct.keys():
            if isinstance(dct[k], dict) and key in dct[k]:
                return dct[k][key]

    return None


def extract_digits(line: str) -> int:
    """Extract alle cijfers uit een string."""
    return int(re.sub(r'[^0-9]', '', line))


def extract_phonenr(line: str) -> str:
    """Extract telefoonnummer uit string."""
    return re.sub(r'[^+0-9]', '', line)


class Response:
    link: str
    _browser: Browser
    page_source: str
    soup: BeautifulSoup

    def __init__(self, link: str, headless: bool = True):
        self.link = link
        self._browser = Browser(headless=headless).__enter__()

    def get_response(self, wait_for_elements: list = None):
        self._browser.driver.get(self.link)

        if wait_for_elements:
            for el in wait_for_elements:
                self.add_wait_for_element(el)

        self.page_source = self._browser.driver.page_source
        self._browser.driver.close()

    def create_soup(self):
        self.soup = BeautifulSoup(self.page_source, features='lxml')

    def close(self):
        self._browser.driver.quit()

    def add_wait_for_element(self, xpath_elem):
        """Wait for element to appear on website."""
        try:
            element_present = ec.presence_of_element_located((By.XPATH, xpath_elem))
            WebDriverWait(self._browser.driver, 10).until(element_present)
            sleep(0.20)
        except TimeoutException:
            print('WARNING: Timed out waiting for page to load')


class Attractie:
    _response: Response
    _xpath_staticmap_element: str = "//img[@class='attractions-attraction-review-location-StaticMap__map--3_EAL']"

    def __init__(self, title=None, ta_id=None, plaats=None, postcode=None, rating=None, straat=None,
                 country=None, aantal_reviews=None, reviews=None, coords=None):
        self._tripadvisor_id: int = ta_id
        self._title: str = title
        self._straat: str = straat
        self._postcode: str = postcode
        self._plaats: str = plaats
        self._country: str = country
        self._coords: Point = coords
        self._rating: float = rating
        self._aantal_reviews: int = aantal_reviews
        self._reviews: str = reviews  # TO DO opsplitsen

    def __repr__(self):
        return f"Titel: {self.title}\n" \
               f"ID: {self.tripadvisor_id}\n" \
               f"Rating: {self.rating}\n" \
               f"Aantal reviews: {self.aantal_reviews}\n" \
               f"Straat: {self.straat}\n" \
               f"Land: {self.country}\n" \
               f"Postcode: {self.postcode}\n" \
               f"Coordinaten: {self.coords}"

    @property
    def response(self):
        return self._response

    @property
    def title(self) -> str:
        return self._title

    @title.setter
    def title(self, value: str):
        self._title = str(value).strip() if value else ''

    @property
    def tripadvisor_id(self) -> int:
        assert isinstance(self._tripadvisor_id, int)
        return self._tripadvisor_id

    @tripadvisor_id.setter
    def tripadvisor_id(self, value: str):
        self._tripadvisor_id = int(value) if value else -1

    @property
    def rating(self) -> float:
        assert isinstance(self._rating, float)
        return self._rating

    @rating.setter
    def rating(self, value):
        self._rating = float(value) if value else -1.0

    @property
    def plaats(self):
        return self._plaats

    @plaats.setter
    def plaats(self, value):
        self._plaats = value if value else ''

    @property
    def straat(self):
        return self._straat

    @straat.setter
    def straat(self, value):
        self._straat = value if value else ''

    @property
    def postcode(self):
        return self._postcode

    @postcode.setter
    def postcode(self, value):
        self._postcode = value if value else ''

    @property
    def aantal_reviews(self):
        return self._aantal_reviews

    @aantal_reviews.setter
    def aantal_reviews(self, value):
        self._aantal_reviews = int(value) if value else -1

    @property
    def coords(self):
        return self._coords

    @coords.setter
    def coords(self, value):
        self._coords = Point(float(v) for v in value) if value and len(value) == 2 else ''

    @property
    def country(self):
        return self._country

    @country.setter
    def country(self, value):
        self._country = value if value else ''

    def from_link(self, link: str, headless: bool = True):
        self._response = Response(link, headless=headless)
        self.response.get_response(
            wait_for_elements=[self._xpath_staticmap_element]
        )
        self.response.create_soup()

    def find_details_in_script_header(self, key: Any):
        script = self.response.soup.find('script', {'type': 'application/ld+json'}).next
        json_dict = json.loads(script)
        return find_value_nested_dict(key, json_dict)

    def find_title(self):
        self.title = self.find_details_in_script_header('name')
        return self

    def find_ta_id(self):
        res = re.search(
            r'^/[\w]+-g([0-9]+)-d([0-9]+)',
            urlparse(self.response.link).path,
            re.IGNORECASE
        )
        self.tripadvisor_id = res.group(2) if res else -1
        return self

    def find_rating(self):
        self.rating = self.find_details_in_script_header('ratingValue')
        return self

    def find_aantal_reviews(self):
        self.aantal_reviews = self.find_details_in_script_header('reviewCount')
        return self

    def find_adres_straat(self):
        self.straat = self.find_details_in_script_header('streetAddress')
        return self

    def find_postcode(self):
        self.postcode = self.find_details_in_script_header('postalCode')
        return self

    def find_plaats(self):
        self.plaats = self.find_details_in_script_header('addressLocality')
        return self

    def find_country(self):
        self._country = self.find_details_in_script_header('addressCountry').get('name')
        return self

    def find_coords(self):
        coords = self.response.soup \
            .find('img', {'src': lambda x: str(x).startswith('https://maps')}).get('src', None)
        if coords:
            coords = parse_qs(coords).get('center', [''])[0]
            self.coords = coords.split(',')
        else:
            return []


"""
def get_data3(link: str, driver):
    ""Verwerk alle data van website <link>.""
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
"""