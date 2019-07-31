
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options


def get_functies():
    page = BeautifulSoup(
        requests.get('http://www.meesterbaan.nl/').content,
        features='lxml'
    )
    links = page.findAll(
        'select', {'id': 'ctl00_plhControl_VacaturesZoekenHomeXml1_ddlFuncties'})
    return [f.lower() for f in links[0].text.split('\n')[2:] if f != '']


DRIVER_PATH = r'Documents\geckodriver.exe'
OUTPUT_FILE = 'meester_baan.csv'
COLUMNS = [
    'Naam vacature',
    'Naam school',
    'Plaats',
    'Sector',
    'Denominatie',
    'Dienstverband',
    'Functietitel',
    'FTE',
    'Opleiding',
    'Salarisschaal',
    'Plaatsingsdatum'
]

START_URL = "https://www.meesterbaan.nl/vacatures/docent/alle%20functies/" \
            "alle%20regio's?doelgroep=2&id_sector=-1&filter=&s=Datum"
# voor koppelen andere bestanden (evt pad aanpassen):
DIR = ""


def init_driver(headless=False, start_url=''):
    options = Options()
    options.headless = headless
    firefox = Firefox(executable_path=DRIVER_PATH, options=options)
    firefox.get(start_url)
    print("Driver geinitialiseerd (headless={})".format(headless))
    print("URL: {}".format(start_url))
    return firefox


def get_urls(soup):
    links = soup.findAll('a')
    urls_set = set()

    for link in links:
        if 'hplLeesMeer"' in str(link):
            clean_link = str(link).split('href="')[1].split('?')[0]
            urls_set.add(clean_link)

    return list(urls_set)


def soup_find_attr(content_soup, attr_dict):
    """functie om get_info op te schonen"""
    soup = content_soup.find('span', attrs=attr_dict)
    if soup:
        return str(soup.text).strip('\"').strip()
    else:
        return ''


def get_info(content_soup):
    return [
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_lblTitel'}),
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_lblSchool'}),
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_lblPlaats'}),
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_lblSector'}),
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_lblDenominatie'}),
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_lblDienstverband'}),
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_lblDienstverband'}),
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_lblWTF'}),
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_txtBevoegdheden'}),
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_lblSalarisSchalen'}),
        soup_find_attr(content_soup, {'id': 'ctl00_plhControl_lblPlaatsing2'})
    ]


def driver_update_form_action(driver):
    # bug in code website: form_action postback wordt anders bij elke volgende pagina
    # steeds langer, tot dat deze bij pagina 46 niet meer update

    form_element = driver.find_element_by_xpath("//form[@name='aspnetForm']")
    driver.execute_script(
        "arguments[0].setAttribute('action','./alle regio%27s?functie=alle+functies" +
        "&regio=alle+regio%27s&doelgroep=2&id_sector=-1&filter=&s=Datum')",
        form_element)
    return driver


def klik_en_get_urls_range(driver, begin, eind):

    temp_lijst = []

    for j in range(begin, eind+1):

        elem = driver.find_elements_by_xpath("//a[@class='PageNumbers'][%d]" % j)

        if elem:
            driver.find_element_by_xpath("//a[@class='PageNumbers'][%d]" % j).click()
            time.sleep(3)
            driver = driver_update_form_action(driver)
            time.sleep(1)
            temp_lijst.extend(get_urls(BeautifulSoup(driver.page_source,
                                                     features='lxml')))
        else:
            break

    print("{} urls toegevoegd".format(len(temp_lijst)))
    return temp_lijst


def generate_url_list(driver):
    # check op de 1e pagina, of er een volgende set met paginas is
    volgende_pagina = driver.find_elements_by_xpath("//a[@class='PageNumbers'][5]")

    # 1e pagina lezen met resultaten
    url_lijst = get_urls(BeautifulSoup(driver.page_source, features='html.parser'))
    print("{} urls toegevoegd".format(len(url_lijst)))

    # probeer volgende 4 paginas met urls te lezen
    url_lijst.extend(klik_en_get_urls_range(driver, 1, 5))

    # parse rest van de paginas muv van de laatste, als er volgende paginas zijn
    while driver.find_elements_by_xpath("//a[@class='PageNumbers'][6]"):
        url_lijst.extend(klik_en_get_urls_range(driver, 2, 6))

    # check dat het de allerlaatste set met paginas is
    # (en niet nog steeds de eerste pagina)
    if volgende_pagina:
        url_lijst.extend(klik_en_get_urls_range(driver, 2, 5))

    return url_lijst


if __name__ == '__main__':
    start = time.time()
    urls = []
    data_list = []

    driver = init_driver(headless=False, start_url=START_URL)

    urls.extend(generate_url_list(driver))

    print("alle urls ontvangen ({})".format(len(urls)))

    driver.quit()

    for url in urls:

        try:
            response = requests.get(url)
            response.raise_for_status()
            data_list.append(get_info(BeautifulSoup(response.content,
                                                    features='lxml')))

        except requests.exceptions.HTTPError as err:
            print(err)

    end = time.time()
    print('Totale duur webscrape (min): ', round((end - start) / 60, 2))
    print('Totaal aantal items: {}'.format(len(data_list)))

    pd.DataFrame(data_list, columns=COLUMNS).to_csv(OUTPUT_FILE, index=False, sep=';')
    print("Output exported to: {}".format(OUTPUT_FILE))
