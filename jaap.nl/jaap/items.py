import scrapy
import re
from scrapy.loader import ItemLoader
from scrapy.loader.processors import Compose, TakeFirst, Identity, Join

# removes spaces and tabs for text fields
clean_text = Compose(TakeFirst(),
                     lambda v: v.strip(),
                     lambda v: '' if v == '-' else v,
                     lambda v: v.replace('é', 'e'),
                     lambda v: None if v == '' else v)

# only returns numbers and convert to int, else string
clean_int = Compose(TakeFirst(),
                    lambda s: ''.join(re.findall('\d', s)),
                    lambda s: int(s) if s else None,
                    )

get_status = Compose(lambda t: status(t) if t else 'verkoop')
toev_regex = '^[\d|\']*[a-zA-Z\.\+\-]+\d{1,5}[\+\-]*'


def status(line):
    if 'voorbehoud' in line.lower():
        return 'verkocht onder voorbehoud'
    elif 'verkocht' in line.lower():
        return 'verkocht'
    else:
        return 'verkoop'


def is_huisletter(word):
    return word.isalpha() and len(word) == 1 and not is_roman(word)


def is_roman(number):
    if number == 'i' or number == 'ii' or number == 'iii' or number == 'iv' or number == 'v' or number == 'x':
        return True
    else:
        return False


def roman_to_int(number):
    if number == 'i':
        return 1
    if number == 'ii':
        return 2
    if number == 'iii':
        return 3
    if number == 'iv':
        return 4
    if number == 'v':
        return 5
    if number == 'x':
        return 10
    else:
        return number


def convert_hs(str_hs):
    if str(str_hs).strip().lower() == 'hs' or str(str_hs).strip().lower() == 'huis':
        return 'h'
    else:
        return str_hs.lower()


filter_toev = Compose(
    lambda f: f.strip('-').replace(' ', '').replace('&', '').replace(',', ''),
    # lambda f: f.lower() if re.findall('^[a-zA-Z\d -]{0,6}$', str(f)) else '',
    # lambda f: '' if 'ong' in f else f,
    # lambda f: '' if 'ov' in f else f,
    # lambda f: '' if 'pp' in f else f,
    # lambda f: '' if f == '0' else f,
    # lambda f: '' if 'kav' in f else f,
    # lambda f: '' if 'bou' in f else f,
    convert_hs,
    roman_to_int
)


class JaapItem(scrapy.Item):
    soort_woning = scrapy.Field()
    straat = scrapy.Field()
    huisnummer = scrapy.Field()
    huisletter = scrapy.Field()
    toevoeging = scrapy.Field()
    postcode = scrapy.Field()
    plaats = scrapy.Field()
    provincie = scrapy.Field()
    status = scrapy.Field()
    aangeboden_sinds = scrapy.Field()
    huidige_vraagprijs = scrapy.Field()
    oorspr_vraagprijs = scrapy.Field()
    prijs_wijzigingen_data = scrapy.Field()
    prijs_per_m2 = scrapy.Field()
    tijd_in_de_verkoop = scrapy.Field()
    bouwjaar = scrapy.Field()
    oppervlakte = scrapy.Field()
    perceeloppervlakte = scrapy.Field()
    inhoud = scrapy.Field()
    aantal_kamers = scrapy.Field()
    aantal_slaapkamers = scrapy.Field()
    bijzonderheden = scrapy.Field()
    isolatie = scrapy.Field()
    verwarming = scrapy.Field()
    energielabel = scrapy.Field()
    energieverbruik = scrapy.Field()
    staat_onderhoud_binnen = scrapy.Field()
    staat_onderhoud_buiten = scrapy.Field()
    sanitaire_voorzieningen = scrapy.Field()
    keuken = scrapy.Field()
    staat_schilderwerk = scrapy.Field()
    tuin = scrapy.Field()
    uitzicht = scrapy.Field()
    balkon = scrapy.Field()
    garage = scrapy.Field()
    aantal_keer_getoond = scrapy.Field()
    aantal_keer_getoond_gisteren = scrapy.Field()
    bron = scrapy.Field()
    jaap_id = scrapy.Field()
    laatste_scrape = scrapy.Field()


class JaapLoader(ItemLoader):
    default_item_class = JaapItem

    soort_woning_in = Compose(lambda s: s[0] if s else None)
    soort_woning_out = clean_text

    bouwjaar_in = Compose(lambda s: s[1])
    bouwjaar_out = clean_int

    oppervlakte_in = Compose(lambda s: s[2])
    oppervlakte_out = clean_int

    inhoud_in = Compose(lambda s: s[3])
    inhoud_out = clean_int

    perceeloppervlakte_in = Compose(lambda s: s[4])
    perceeloppervlakte_out = clean_int

    bijzonderheden_in = Compose(lambda s: s[5])
    bijzonderheden_out = clean_text

    isolatie_in = Compose(lambda s: s[6])
    isolatie_out = clean_text

    verwarming_in = Compose(lambda s: s[7])
    verwarming_out = clean_text

    energielabel_in = Compose(lambda s: s[8])
    energielabel_out = clean_text

    energieverbruik_in = Compose(lambda s: s[9])
    energieverbruik_out = clean_text

    staat_onderhoud_binnen_in = Compose(lambda s: s[10])
    staat_onderhoud_binnen_out = clean_text

    aantal_kamers_in = Compose(lambda s: s[11])
    aantal_kamers_out = clean_int

    aantal_slaapkamers_in = Compose(lambda s: s[12])
    aantal_slaapkamers_out = clean_int

    sanitaire_voorzieningen_in = Compose(lambda s: s[13])
    sanitaire_voorzieningen_out = clean_text

    keuken_in = Compose(lambda s: s[14])
    keuken_out = clean_text

    staat_onderhoud_buiten_in = Compose(lambda s: s[15])
    staat_onderhoud_buiten_out = clean_text

    staat_schilderwerk_in = Compose(lambda s: s[16])
    staat_schilderwerk_out = clean_text

    tuin_in = Compose(lambda s: s[17])
    tuin_out = clean_text

    uitzicht_in = Compose(lambda s: s[18])
    uitzicht_out = clean_text

    balkon_in = Compose(lambda s: s[19])
    balkon_out = clean_text

    garage_in = Compose(lambda s: s[20])
    garage_out = clean_text

    aantal_keer_getoond_in = Compose(lambda s: s[21])
    aantal_keer_getoond_out = clean_int

    aantal_keer_getoond_gisteren_in = Compose(lambda s: s[22])
    aantal_keer_getoond_gisteren_out = clean_int

    aangeboden_sinds_in = Compose(lambda s: s[0])
    aangeboden_sinds_out = clean_text

    huidige_vraagprijs_in = Compose(lambda s: s[1])
    huidige_vraagprijs_out = clean_int

    oorspr_vraagprijs_in = Compose(lambda s: s[2])
    oorspr_vraagprijs_out = clean_int

    prijs_per_m2_in = Compose(lambda s: '' if not s else s[1])
    prijs_per_m2_out = clean_int

    tijd_in_de_verkoop_in = Compose(lambda s: '' if not s else s[2])
    tijd_in_de_verkoop_out = clean_text

    prijs_wijzigingen_data_in = Compose(
         lambda s: [i.strip('(huidig)').strip('(plaatsing)').strip() for i in s] if 'Prijs' not in s else [],
         lambda s: [i.strip('€').replace('.', '').strip() for i in s],
         Join('/'))
    prijs_wijzigingen_data_out = Join('|')

    status_in = Compose(lambda t: 'verkoop' if not t else t)
    status_out = Compose(Join(), Identity(), get_status)

    plaats_in = Identity()
    plaats_out = Compose(TakeFirst(), lambda s: s.replace('+', ' ').title())

    postcode_in = Identity()
    postcode_out = Compose(TakeFirst(), lambda s: s.upper() if '_' not in s else '')

    straat_in = Identity()
    straat_out = Compose(TakeFirst(), lambda s: s.replace('+', ' ').strip().title())

    # Domein: Lengte 	1..5
    # Domein: Patroon 	Een natuurlijk getal tussen 1 en 99999
    huisnummer_in = Identity()
    huisnummer_out = Compose(clean_int, lambda h: (h if h > 0 else None) if h else None)

    # Domein: Lengte 	1
    # Domein: Patroon 	Een hoofdletter (A – Z) of kleine letter (a – z)
    huisletter_in = Compose(TakeFirst(),
                            lambda h: re.compile(toev_regex).split(h),
                            lambda h: [] if len(h) == 1 else h[1]
                            )
    huisletter_out = Compose(TakeFirst(),
                             lambda h: h.split('+')[0].replace('-', '').strip().lower(),
                             lambda h: h if is_huisletter(h) else ''
                             )

    # Domein: Lengte 	1..4
    # Domein: Patroon 	Maximaal vier alfanumerieke tekens bestaande uit een combinatie van hoofdletters (A – Z),
    # kleine letters (a – z) en/of cijfers (0 – 9)
    toevoeging_in = Compose(TakeFirst(),
                            lambda t: re.compile(toev_regex).split(t),
                            lambda t: [] if len(t) == 1 else t[1]
                            )
    toevoeging_out = Compose(TakeFirst(),
                             lambda t: t.replace('/', '').split('+'),
                             lambda t: list(t[1:]) if len(t) == 2 else t,
                             lambda t: ''.join(t) if len(t) > 1 else ('' if is_huisletter(t[0]) else t[0]),
                             filter_toev
                             )

    provincie_in = Identity()
    provincie_out = Compose(TakeFirst(), lambda s: s.replace('+', ' ').title())

    bron_in = Identity()
    bron_out = Compose(TakeFirst(), lambda s: s.strip('www.'))

    jaap_id_in = Identity()
    jaap_id_out = clean_int

    laatste_scrape_in = Identity()
    laatste_scrape_out = Compose(TakeFirst(), lambda s: s.strftime('%d-%m-%Y'))
