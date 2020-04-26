import json
import re
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse, ParseResult

from shapely.geometry import Point

from tripadvisor.browser import Response

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


def extract_integer(line: str) -> int:
    """Extract alle cijfers uit een string."""
    if isinstance(line, int):
        return line
    try:
        return int(re.sub(r'[^0-9]', '', line))
    except (TypeError, ValueError, AttributeError):
        return -1


def extract_float(line) -> float:
    if isinstance(line, float):
        return line

    elif isinstance(line, int):
        return float(line)

    else:
        try:
            return float(re.sub(r'[^0-9.]', '', line.replace(',', '.')))
        except (TypeError, ValueError, AttributeError):
            return -1.0


def extract_phonenr(line: str) -> str:
    """Extract telefoonnummer uit string."""
    return re.sub(r'[^+0-9]', '', line)


class Attractie:
    _response: Response

    _xpath_staticmap_element: str = "//img[contains(@src, 'maps.google')]"
    _tripadvisor_id: int
    _title: str
    _straat: str
    _postcode: str
    _plaats: str
    _country: str
    _coords: Point
    _rating: float
    _aantal_reviews: int
    _reviews: list
    _link: ParseResult

    def __init__(self, link: str, headless: bool = True):
        self.link = link
        self.get_attractie(headless=headless)

    def __repr__(self):
        return f"Titel: {self.title}\n" \
               f"ID: {self.tripadvisor_id}\n" \
               f"Rating: {self.rating}\n" \
               f"Aantal reviews: {self.aantal_reviews}\n" \
               f"Reviews: {self.reviews}\n" \
               f"Straat: {self.straat}\n" \
               f"Land: {self.country}\n" \
               f"Postcode: {self.postcode}\n" \
               f"Coordinaten: {self.coords}"

    @property
    def link(self) -> ParseResult:
        return self._link

    @link.setter
    def link(self, value: str):
        try:
            link = urlparse(f"{URL}{value}")

        except (AttributeError, TypeError, ValueError):
            print(f'geen geldige url: {value}')
            self._link = urlparse('')

        else:
            self._link = link

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
        return self._tripadvisor_id

    @tripadvisor_id.setter
    def tripadvisor_id(self, value: str):
        self._tripadvisor_id = extract_integer(value)

    @property
    def rating(self) -> float:
        return self._rating

    @rating.setter
    def rating(self, value: str):
        self._rating = extract_float(value)

    @property
    def plaats(self) -> str:
        return self._plaats

    @plaats.setter
    def plaats(self, value: str):
        self._plaats = value if value else ''

    @property
    def straat(self) -> str:
        return self._straat

    @straat.setter
    def straat(self, value: str):
        self._straat = value if value else ''

    @property
    def postcode(self) -> str:
        return self._postcode

    @postcode.setter
    def postcode(self, value: str):
        self._postcode = value if value else ''

    @property
    def aantal_reviews(self) -> int:
        return self._aantal_reviews

    @aantal_reviews.setter
    def aantal_reviews(self, value: str):
        self._aantal_reviews = extract_integer(value)

    @property
    def coords(self) -> Point:
        return self._coords

    @coords.setter
    def coords(self, value):
        try:
            lon = extract_float(value[0])
            lat = extract_float(value[1])
        except (IndexError, TypeError):
            lat, lon = -1, -1
        self._coords = Point(lon, lat)

    @property
    def country(self) -> str:
        return self._country

    @country.setter
    def country(self, value: str):
        self._country = value if value else ''

    @property
    def reviews(self) -> list:
        return self._reviews

    @reviews.setter
    def reviews(self, value):
        self._reviews = [extract_integer(r.string) for r in value] if value else [-1] * 5

    @property
    def data(self) -> tuple:
        attrac = (
            'NEW',
            self.title,
            self.link.path,
            self.tripadvisor_id,
            self.rating,
            self.straat,
            self.postcode,
            self.plaats,
            self.country,
            self.aantal_reviews,
            *self.reviews,
            self.coords.x,
            self.coords.y
        )
        if len(attrac) != 17:
            print('Warning: attractie != 17')

        self.print_()
        return attrac

    def print_(self):
        print(
            f"Attractie(titel={self.title}, "
            f"id={self.tripadvisor_id}, "
            f"rating={self.rating}, "
            f"aantal_reviews={self.aantal_reviews}, "
            f"coord={self.coords}, "
            f"link={self.link.path})"
         )

    def from_link(self, headless: bool = True):
        self._response = Response(self.link.geturl(), headless=headless, init=True)

        wait = [
            (self._xpath_staticmap_element, 0.1),  # works: 1, 0.5
            ("//span[@class='_82HNRypW']", 0.1)  # works: 0.5, 0.25
        ]
        self.response.get_response(wait_for_elements=wait)
        self.response.create_soup()

    def find_details_in_script_header(self, key: Any) -> dict:
        try:
            script = self.response.soup.find('script', {'type': 'application/ld+json'})
            value = find_value_nested_dict(key, json.loads(script.string)) or {}

        except (TypeError, AttributeError):
            return {}

        else:
            return value

    def find_title(self):
        title = self.find_details_in_script_header('name')

        if not title:
            try:
                title = self.response.soup.find('span', {'class': 'IKwHbf8J'}).get_text(strip=True)
            except AttributeError:
                pass

        self.title = title

    def find_ta_id(self):
        try:
            res = re.search(
                r'^/[\w]+-g([0-9]+)-d([0-9]+)',
                self.link.path,
                re.IGNORECASE
            ).group(2)

        except (AttributeError, ValueError, TypeError):
            self.tripadvisor_id = -1

        else:
            self.tripadvisor_id = res

    def find_rating(self):
        if self.aantal_reviews == -1:  # Geen reviews, geen rating.
            rating = None

        else:
            rating = self.find_details_in_script_header('ratingValue')

            if not rating:
                try:
                    content = self.response.get_css_properties(
                        elem=["span.uq1qMUbD._2n4wJlqY", "span.uq1qMUbD._2vB__cbb"],
                        by='css',
                        prop="content",
                        pseudo=':after'
                    )
                    full = [repr(f).count("\\ue129") for f in content]
                    half = [repr(h).count("\\ue12a") * 5 for h in content]  # 0 of 1
                    rating = f'{min(full)}.{max(half)}'

                except (TypeError, ValueError):
                    # min/max throw valueerror on empty list
                    rating = None

        self.rating = rating

    def find_aantal_reviews(self):
        aantal_reviews = self.find_details_in_script_header('reviewCount')

        if not aantal_reviews:
            try:
                aantal_reviews = self.response.soup.find('span', {'class': '_82HNRypW'}).get_text()
            except AttributeError:
                pass

        self.aantal_reviews = aantal_reviews

    def find_adres_straat(self):
        self.straat = self.find_details_in_script_header('streetAddress')

    def find_postcode(self):
        self.postcode = self.find_details_in_script_header('postalCode')

    def find_plaats(self):
        self.plaats = self.find_details_in_script_header('addressLocality')

    def find_country(self):
        coun = self.find_details_in_script_header('addressCountry')
        self.country = coun.get('name', None)

    def find_coords(self):
        def find_map(x):
            return str(x).startswith('https://maps')

        # [i['src'] for i in self.response.soup.find_all('img') if 'src' in i.attrs]
        try:
            coords = self.response.soup.find('img', {'src': find_map})

            if coords:
                coords = coords.get('src', None)

            else:
                imgs = [i['src'] for i in self.response.soup.find_all('img')
                        if 'src' in i.attrs and i.get('src', '').startswith('/data/1.0/maps')]
                coords = urlparse(imgs[0]).query if imgs else None

            coords = parse_qs(coords)['center']
            coords = str(coords).split(',')

        except (AttributeError, IndexError, TypeError, KeyError, ValueError):
            self.coords = None

        else:
            self.coords = coords

    def find_reviews(self):
        try:
            soup = self.response.soup
            reviews = soup.find_all(
                'span', {'class': 'location-review-review-list-parts-ReviewRatingFilter__row_num--3cSP7'}
            )

            if not reviews:
                reviews = soup.find_all('span', {'class': 'eqh_0ztw'})

        except AttributeError:
            self.reviews = []

        else:
            self.reviews = reviews

    def get_attractie(self, headless: bool):
        self.from_link(headless=headless)

        self.find_coords()
        self.find_title()
        self.find_aantal_reviews()
        self.find_rating()
        self.find_adres_straat()
        self.find_country()
        self.find_plaats()
        self.find_postcode()
        self.find_ta_id()
        self.find_reviews()
