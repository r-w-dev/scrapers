import re
import warnings
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings()
pd.set_option('max_colwidth', 150)
pd.set_option('display.max_rows', 50)

work_dir = Path(r'C:\Users\Roel\PycharmProjects\scrapers\meesterbaan')
json_folder = work_dir / 'output' / 'json'
pickle_folder = work_dir / 'output' / 'pickle'


def get_json_from_url(url, results_key: str = None, encoding: str = 'utf-8'):
    with requests.get(url, verify=False) as resp:
        if resp.status_code != 200:
            if resp.status_code == 500:
                print(f'HTTP error: 500 Server Error {url}')
                return None
            else:
                resp.raise_for_status()

        resp.encoding = encoding
        result = resp.json()

    if results_key and results_key not in result:
        raise KeyError(f'"{results_key}" key not in result')
    else:
        return result[results_key]


def split_brin(brin6: str) -> tuple:
    return str(brin6[:4].upper()), str(brin6[4:].upper())


def make_brin6(brin4, vnr) -> str:
    vnr = str(vnr)
    if len(vnr) == 1:
        vnr = f'0{vnr}'

    return f'{brin4.upper()}{vnr}'


class Verwerken:
    ENCODING = 'windows-1252'
    NA_VAL = '< onbekend >'
    FTE_NA = '0,7'
    FTE_CSV = work_dir / 'oud' / 'webscraper-vacatures-master' / 'FTE_hercoderen.csv'
    MRA_CSV = work_dir / 'oud' / 'webscraper-vacatures-master' / 'MRA_plaatsen.csv'

    EXPORT_COLS = [
        'denominatie',
        'dienstverband',
        'fte_orig',
        'functie_titel',
        'duo_naam_kort',  # placeholder
        'duo_naam_volledig',  # placeholder
        'duo_naam',  # placeholder
        'naam_school',
        'naam_vacature',
        'opleiding',
        'peil_datum',
        'plaats',
        'plaatsings_datum',
        'salaris_schaal',
        'sector',
        'tijd_online_dagen',
        'adres',
        'locatie',  # placeholder
        'fte_new'
    ]

    def __init__(self, file_path: str):
        self.file_path = file_path
        self._data = self.read_json(self.file_path)

    def __repr__(self):
        return f'Verwerken({self.file_path})'

    def create_not_existing_cols(self, col_list: list, placeholder='< placeholder >'):
        to_create = set(col_list).difference(self.data.columns)
        self._assign({c: placeholder for c in to_create})

    def read_json(self, jsonfile):
        if not Path(jsonfile).exists():
            jsonfile = work_dir / 'meesterbaan' / jsonfile

            if not jsonfile.exists():
                raise FileNotFoundError(f'{Path(jsonfile)}')
        return pd.read_json(jsonfile, encoding=self.ENCODING).fillna(pd.NA)

    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, val):
        if isinstance(val, str):
            val = Path(val)
        self._file_path = val

    @property
    def data(self):
        return self._data

    def _assign(self, kwargs: dict):
        self._data = self.data.assign(**kwargs)

    def plaatsings_datum(self):
        self._assign(
            {'plaatsings_datum': pd.to_datetime(self.data.plaatsings_datum, format='%d-%m-%Y')}
        )

    def peil_datum(self):
        today = pd.Timestamp('today')
        self._assign({'peil_datum': pd.Timestamp(today.year, today.month, today.day)})

    def tijd_online_dagen(self):
        self._assign({'tijd_online_dagen': self.data.peil_datum - self.data.plaatsings_datum})

    def opleiding(self):
        def _apply_in_lower(txt):
            return self.data['opleiding'].fillna(self.NA_VAL).apply(lambda x: txt in x.lower())

        self._assign({
            'opl_eerste_graads': _apply_in_lower('1e'),
            'opl_tweede_graads': _apply_in_lower('2e'),
            'opl_pabo': _apply_in_lower('pabo'),
            'opl_leraar_in_opleiding': _apply_in_lower('opleiding'),
            'opl_overig': _apply_in_lower('overige'),
            'opl_buitenlands': _apply_in_lower('buitenlands'),
            'opl_onbekend': _apply_in_lower('onbekend')
        })

    def filter_mra(self):
        mra = set(
            pd.read_csv(
                self.MRA_CSV,
                sep=';',
                encoding=self.ENCODING,
                converters={'Woonplaatsen': str.lower},
                usecols=['Woonplaatsen'],
                squeeze=True
            )
        ).union({
            'abcoude',
            'almere-buiten',
            'amsterdam zo',
            'amsterdam zuidoost',
            'driehuis',
            'driehuis nh'
        })
        plaats_replace = {
            'Amsterdam Zo': 'Amsterdam',
            'Amsterdam Zuidoost': 'Amsterdam',
            'Amsteram': 'Amsterdam',
            'Driehuis': 'Driehuis Nh'
        }
        self._data = self.data.loc[self.data['plaats'].str.lower().isin(mra)]
        self._assign({'plaats': lambda x: x['plaats'].str.title().replace(plaats_replace)})

    def filter_onderwijs_soort(self):
        self._data = self.data.loc[
            self.data['sector'].str.contains(
                "Speciaal (Basis) Onderwijs|"
                "Basisonderwijs|"
                "Voortgezet onderwijs|"
                "Middelbaar beroepsonderwijs|"
                "Voortgezet speciaal onderwijs",
                case=False
            )]

    def fte_hercoderen(self):
        fte_codes = pd.read_csv(self.FTE_CSV, sep=';', encoding=self.ENCODING) \
            .applymap(lambda x: str(x).lower())

        self._assign({
            'fte':
                self.data['fte']
                .fillna(self.FTE_NA)
                .apply(lambda x: str(x).lower())
                .replace(dict(fte_codes.values))
        })

        regex_str = r'(\d{1,4}[\,\.]?\d{0,4})'

        fte = self.data['fte'] \
            .str.extractall(regex_str) \
            .reset_index() \
            .replace(regex=',', value='.') \
            .astype({0: float})

        fte.columns = ['index', 'nummer', 'value']

        self._data = self.data \
            .join(
                fte
                .pivot_table(values='value', index='index', aggfunc='mean')
                .applymap(lambda x: x / 40 if x > 2 else x)
                .round(2)
            ).rename(columns={'fte': 'fte_orig', 'value': 'fte_new'})

    def schoolnaam_replace(self):
        replace_naam = {
            "St. Nicolaaslyceum": "Scholengemeenschap Sint Nicolaas Lyceum voor Lyceum en Havo",
            'Havo De Hof': "Locatie, De Hof",
            'Daltonschool Neptunus': 'Oecumenische Dalton bassischool Neptunus',
            'Cheider': 'Stichting Joodse Kindergemeenschap Cheider',
            'De Apollo': 'locatie, De Apollo',
            'Oscar Carré': 'Basisschool Oscar Carre',
            'De Bïenkorf': 'Basisschool De Bienkorf',
            'St. Jan': 'St Janschool',
            'Daltonschool Aldoende': 'Aldoende',
            'basisschool Polsstok': 'Polsstok',
            'JSG Maimonides': 'Joodse Scholengemeenschap Maimonides voor Ath Mavo En Afd Havo',
            'Boekmanschool Dr. E. hoofdvestiging': 'Dr E Boekman',
            'de Henricus': 'Rooms Katholieke Basisschool Sint Henricusschool',
            'De Ark': 'Oecumenische Basisschool De Ark',
            'BS Elout': 'Basisschool de Elout',
            '3e Daltonschool Alberdingk Thijm': 'Alberdingk Thijmschool Daltonschool',
            'Montessori Lyceum Amsterdam': 'Montessori Scholengemeenschap Amsterdam voor Lyceum Havo Mavo Vbo Lwoo',
            'Mgr. Bekkersschool': 'Basisschool Mgr Bekkers',
            'Kunstmagneetschool De Kraal': 'De Kraal',
            'Open Schoolgemeenschap Bijlmer': 'Open Sgm Bylmer',
            'Alles in 1 school De Zeeheld': 'Brede School Zeeheld',
            'Christelijke Scholengemeenschap Buitenveldert': 'Chr Sgm Buitenveldert',
            'basisschool Klaverblad': 'Klaverblad',
            'De Nicolaas Maesschool ': 'Nicolaas Maes',
            'Al Maes': 'Basisschool Al Maes',
            'al Maes': 'Basisschool Al Maes',
            'De Buikslotermeer': 'Buikslotermeer',
            'Basisschool Annie M G Schmidt': 'Brede School Annie MG Schmidt',
            'Obs JP Coen': 'Jan Pietersz. Coenschool',
            '2e Daltonschool Pieter Bakkum': 'Pieter Bakkumschool Daltonschool',
            'De Vlaamse Reus': 'Openbare Basisschool de Vlaamse Reus',
            'Montessori Boven \'t IJ': 'Basisschool Boven \'t IJ Montessori-School',
            'Dalton IKC Zeven ZeeÃ«n': 'IKC Zeven ZeeÃ«n',
            'IKC NoordRijk ': 'OBS NoordRijk IKC',
            'Montessorischool de Eilanden': 'Stichting Montessori Basisschool de Eilanden',
            'Kindcentrum De Kaap': 'Openbare Basisschool De Kaap',
            'De IJdoornschool': 'IJdoorn-School',
            'Huibersschool': 'Basisschool Huibers',
            'Brede School de Springplank': 'Katholieke Basisschool De Springplank',
            'IJpleinschool': 'Openbare Bassischool IJPlein',
            'Barbaraschool': 'Sint Barbara-School Basisschool',
            'KPC De Atlant': 'Kolom Praktijkcollege De Atlant',
            'BS Fiep Westendorp': 'Brede school Fiep Westendorp',
            'Jenaplanschool Atlantis': 'Oecumenische Jenaplan basisschool Atlantis',
            'Kameleon': 'Openbare Basisschool De Kameleon',
            'Buitenhout College': 'Oostvaarders College voor Lyceum Havo Mavo Vbo Lwoolocatie Buitenhout',
            'Polderhof': 'Basisschool De Polderhof',
            'Letterland': 'OBS Letterland',
            'Het Palet': 'Openbare Basisschool Het Palet',
            'Aurora': 'Openbare Basisschool Aurora',
            'Argonaut': 'Basisschool De Argonaut',
            'LUMION': 'Calandlyceum voor Gymnasium Atheneum Havo Vmbo locatie,  Lumion',
            'Joseph Lokinschool': 'Joseph Lokin Basisschool',
            'Praktijkschool Oost ter Hout': 'School voor Praktische Vorming Oost-ter-Hout',
            'Lyceum Sancta Maria': 'Sancta Maria Lyceum voor Havo, Atheneum en Gymnasium',
            'Technisch College Velsen/Maritiem College IJmuiden':
                'Noordzee Onderwijs Groep- locatie Technisch College Velsen',
            'Trias VMBO': 'Scholengroep Krommenie - locatie Trias VMBO',
            'ISG Arcus': 'Interconfessionele Scholengemeenschap Arcus voor Atheneum Havo Mavo Vbo Lwoo',
            'SG Antoni GaudÃ­': 'Purmerendse Scholengroep locatie SG. Antoni Gaudi',
            'SWV VO Waterland': 'Waterlandschool',
            'Jan van Egmond Lyceum': 'Purmerendse Scholengroep locatie Jan v. Egmond Lyceum',
            'Pascal Zuid': 'Pascal College  voor Vwo Havo Mavo Vbo Lwoo',
            'Sint-Vituscollege': 'Sint-Vitusmavo',
            'Basisschool Al Ihsaan': 'Al-Ihsaan',
            'De Josephschool': 'Rooms Katholieke Basisschool St Joseph',
            'De School': 'Sociocratische school De School',
            'De Egelantier': 'Openbare Basisschool De Egelantier',
            'de Wilge': 'Rooms Katholieke Basisschool De Wilge',
            'Coornhert Lyceum ': 'Coornhertlyc Gem Scholengemeenschap Lyceum Havo Mavo',
            'Basisschool oostelijke eilanden BOE': 'Basisschool Oostelijke Eilanden',
            'De Savornin Lohman': 'Savornin Lohman',
            'Hermann Wesselink College': 'H Wesselink College',
            'Luzac Hilversum': 'Luzac Lyceum Hilversum',
            'Mediacollege Amsterdam': 'Stichting Media Amsterdam',
            'Altra College Zuidoost / Altra Werkt': 'Altra College Zuidoost',
            'HMC mbo vakschool | Amsterdam': 'Hout en Meubileringscollege Mbo',
            'Altra College Centrum': 'Altra College',
            'Altra': 'Altra College',
            'Almeerse Scholen Groep': 'ASG',
            'Het Baken Almere': 'Het Baken',
            'Het ABC': 'ABC Onderwijs Adviseurs',
            'Ichthus Lyceum': 'Noordzee Onderwijs Groep voor Gymnasium, Atheneum, Havo, Vmbo',
            'Calvijn College': 'locatie Calvijn College',
            'Luzac Haarlem': 'Luzac College Haarlem',
            'RK Antoniusschool': 'Rooms Katholieke Basisschool Antonius',
            'St. Michaël College': 'RK SGM St Michaelcollege',
            'Cartesius Lyceum': 'Esprit Scholengroep voor Lyceum Havo Mavo Vbo Lwoo',  # geeft meerder matches
            'Berlage Lyceum': 'Berlage',
            'Stichting SchOOL': 'Stg SchOOL',
            'SG Panta Rhei Amstelveen': 'sg Panta Rhei',
            'Dunamare Bestuursbureau': 'Stichting Dunamare Onderwijsgroep',
            'Almeerse Scholen Groep - Vakgr bewegingsonderwijs': 'ASG',
            'Vestiging Gunning op de Daaf Geluk': 'LCH locatie Daaf Gelukschool',
            'Scholengemeenschap Lelystad': 'SGM Lelystad',
            'St. Aloysius College': 'Verenigde Scholen J.A. Alberdingk Thijm VO. Locatie: St. Aloysius College',
            'Vituscollege': 'St Vituscollege',
            'Kennemer College Beroepsgericht': 'Kennemer College',  # 27MD02
            'Bertrand Russell College': 'Scholengroep Krommenie - locatie Bertrand Russell College',
            'Vox college': 'Loc, Vox College',
            'SG De Triade': 'Atlas College voor Lyceum Havo Mavo Vbo Lwoo Locatie De Triade',
            'Helen Parkhurst': 'Openbare Scholengemeenschap Echnaton locatie Helen Parkhurst',
            'Winford Amsterdam Bilingual (Basisschool)': 'Winford Bilingual Primary School',
            'Coornhert Lyceum': 'Coornhertlyc Gem Scholengemeenschap Lyceum Havo Mavo',
            'Osdorpse Montessorischool': 'Osdorpse Montessori School'
        }
        replace_naam = {k.lower(): v.lower() for k, v in replace_naam.items()}
        self._assign({'naam_school_repl': self.data['naam_school'].str.lower().replace(replace_naam)})

    def to_pickle(self, output=None):
        if not output:
            output = pickle_folder / (self.file_path.stem + '.pickle')

        self.create_not_existing_cols(self.EXPORT_COLS, placeholder=pd.NA)
        self.data[self.EXPORT_COLS].to_pickle(str(output))
        print(f'Pickle output: {output}')

    def to_excel(self, output=None):
        if not output:
            output = work_dir / 'output' / 'excel' / (self.file_path.stem + '.xlsx')

        self.create_not_existing_cols(self.EXPORT_COLS, placeholder='')

        with pd.ExcelWriter(
            path=output,
            engine='xlsxwriter',
            datetime_format='DD-MM-YYYY hh:mm:ss',
            date_format='DD-MM-YYYY'
        ) as writer:
            self.data[self.EXPORT_COLS] \
                .assign(
                    plaatsings_datum=self.data.plaatsings_datum.dt.date,
                    peil_datum=self.data.peil_datum.dt.date,
                    tijd_online_dagen=self.data.tijd_online_dagen.dt.days
                ) \
                .to_excel(writer, float_format='%g', index=False, encoding=self.ENCODING)

            worksheet = writer.sheets['Sheet1']
            worksheet.set_column('A:S', 20)
            print(f'Excel output: {output}')

    def run(self, export: bool = False, output=None):
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=UserWarning)

            self.plaatsings_datum()
            self.peil_datum()
            self.tijd_online_dagen()
            self.opleiding()
            self.filter_mra()
            self.filter_onderwijs_soort()
            self.fte_hercoderen()
            self.schoolnaam_replace()

        if export:
            self.to_pickle(output)
            self.to_excel(output)
        return self


def validate_naam(naam):
    return str(naam).strip()


def validate_brin4(brin4, error):
    if brin4:
        brin4 = str(brin4).strip()

        if len(brin4) != 4 or not brin4[:2].isdigit() or not brin4[-2:].isalpha():
            if error == 'raise':
                raise ValueError(f'brin4 fout: {brin4}')
            else:
                return None
    return brin4


def validate_brin6(brin6, error):
    if brin6:
        brin6 = str(brin6).strip()
        brin6 = make_brin6(brin6[:4], brin6[5:])

        if len(brin6) != 6 or not brin6[:2].isdigit() or not brin6[2:4].isalpha() or not brin6[-2:].isdigit():
            if error == 'raise':
                raise ValueError(f'brin6 fout: {brin6}')
            else:
                return None
    return brin6


class Schoolwijzer:

    url_base = 'https://schoolwijzer.amsterdam.nl/nl/api/v1'
    url_lijst = 'lijst'
    url_detail = 'detail'
    url_brin4 = 'brin'
    url_brin6 = 'vestigingsnummer'
    url_zoek = 'zoek'
    url_duo = 'duo_compatible/1'
    ENCODING = 'windows-1252'

    def _built_url(self, naam, brin4, brin6, soort_ow) -> str:
        if brin6:
            br1, br2 = split_brin(brin6)
            br2 = br2[-1] if len(br2) == 2 else br2
            return f'{self.url_base}/{self.url_detail}/{soort_ow}/{self.url_brin4}/{br1}/{self.url_brin6}/{br2}'

        elif brin4:
            br1, _ = split_brin(brin4)
            return f'{self.url_base}/{self.url_detail}/{soort_ow}/{self.url_brin4}/{br1}'

        elif naam:
            res = f'{self.url_base}/{self.url_lijst}/{soort_ow}/{self.url_zoek}/{quote(naam)}'
            return res + f'/{self.url_duo}' if soort_ow != 'so' else res
        else:
            raise ValueError('Geef minimaal 1 paramater op.')

    def find_schools(self, namen=None, brins4=None, brins6=None, **kwargs):
        if brins6:
            return [self.find_school(brin6=br, **kwargs) for br in brins6]
        if brins4:
            return [self.find_school(brin4=br, **kwargs) for br in brins4]
        if namen:
            return [self.find_school(naam=br, **kwargs) for br in namen]

    def find_school(
            self,
            naam: str = None,
            brin4: str = None,
            brin6: str = None,
            soorten_onderwijs: list = ('po', 'vo', 'so'),
            copy=False,
            fields: list = None,
            transpose: bool = True,
            error: str = 'raise'
    ):
        zoekterm = brin6 or brin4 or naam
        print(f'zoekterm: "{zoekterm}" | ', end='')
        if not zoekterm:
            return pd.NA

        naam = validate_naam(naam)
        brin4 = validate_brin4(brin4, error=error)
        brin6 = validate_brin6(brin6, error=error)

        urls = {ow: self._built_url(naam, brin4, brin6, ow) for ow in soorten_onderwijs}

        for soort_ow, url in urls.items():
            result = get_json_from_url(url, results_key='results', encoding=self.ENCODING)

            # case 0 results
            if not result:
                print(f'{soort_ow}: geen resultaten |', end=' ')
                continue

            print(f"Aantal resultaten: {len(result)} \t\t({url})")

            # case 1 result
            if len(result) == 1:
                first = result[0]
                # print(f"'{first['naam']}' gevonden ({soort_ow})")

                return self.prepare_result(result=first, copy=copy, fields=fields, transpose=transpose)

            # case > 1 result
            else:
                print('meerdere resultaten: ')

                for i, res in enumerate(result):
                    print(f"{i}: {make_brin6(res['brin'], res['vestigingsnummer'])}\t{res['naam']}      "
                          f"({res['adres']['adres']}, {soort_ow})")

                numr = int(input('return resultaat nr: '))
                return self.prepare_result(result=result[numr], copy=copy, fields=fields, transpose=transpose)
        else:
            print("Geen match gevonden.")
            return pd.NA

    @staticmethod
    def prepare_result(result, copy: bool = False, fields: list = None, transpose: bool = False):
        all_ = pd.json_normalize(result)
        all_ = all_.rename(columns={c: c.replace('adres.', '').replace('coordinaten.', '') for c in all_.columns})

        if fields is None:
            return all_.T if transpose else all_

        else:
            if copy:
                print(' --- COPIED --- ')
                all_.T.to_clipboard(index=False, header=False)
            else:
                return all_[fields].squeeze()


class Duo:
    RES_IDS = {
        'po': 'cf54f601-d347-4a5c-9942-6ff3dfcf4435',
        'po_gezag': '313b185e-5658-428c-9b82-c35b591df31b',
        'spec': 'f1134aa1-ef13-4d98-8b3c-5affe49a4b47',
        'spec_gezag': '49a7188d-ed8d-4668-8d92-85efe8733219',
        'vo': 'bf9ee0a4-264e-4fa6-8be4-b06e6c4a8133',
        'vo_gezag': '28e20fd7-a5f1-450a-8ecb-11129dece3cc',
        'mbo': 'c4c8f707-7568-4a76-8c20-ee008dc895b7',
        'mbo_gezag': '4be846f1-2608-4ee3-b472-04d737f112a8'
    }

    ENCODING = 'windows-1252'
    BASE_URL = 'https://onderwijsdata.duo.nl/api/3/action/datastore_search?resource_id='
    BASE_SQL_URL = 'https://onderwijsdata.duo.nl/api/3/action/datastore_search_sql?sql='

    DUO_COLUMNS = [
        '_id',
        'PROVINCIE',
        'BEVOEGD GEZAG NUMMER',
        'BRIN NUMMER',
        'VESTIGINGSNUMMER',
        'VESTIGINGSNAAM',
        'STRAATNAAM',
        'HUISNUMMER - TOEVOEGING',
        'POSTCODE',
        'PLAATSNAAM',
        'GEMEENTENUMMER',
        'GEMEENTENAAM',
        'DENOMINATIE',
        'TELEFOONNUMMER',
        'INTERNETADRES',
        'ONDERWIJSSTRUCTUUR',
        'STRAATNAAM CORRESPONDENTIEADRES',
        'HUISNUMMER - TOEVOEGING CORRESPONDENTIEADRES',
        'POSTCODE CORRESPONDENTIEADRES',
        'PLAATSNAAM CORRESPONDENTIEADRES',
        'NODAAL GEBIED CODE',
        'NODAAL GEBIED NAAM',
        'RPA - GEBIED CODE',
        'RPA - GEBIED NAAM',
        'WGR - GEBIED CODE',
        'WGR - GEBIED NAAM',
        'COROPGEBIED CODE',
        'COROPGEBIED NAAM',
        'ONDERWIJSGEBIED CODE',
        'ONDERWIJSGEBIED NAAM',
        'RMC - REGIO CODE',
        'RMC - REGIO NAAM',
        'rank VESTIGINGSNAAM',
        'soort_ow',
        'BEVOEGD GEZAG NAAM',
        'ADMINISTRATIEKANTOORNUMMER',
        'KVK - NUMMER',
        'rank BEVOEGD GEZAG NAAM'
    ]

    def _build_urls(
            self,
            naam: str = None,
            plaats: str = None,
            website: str = None,
            brin4: str = None,
            brin6: str = None,
            adres: str = None
    ):
        urls = {}

        for soort_ow, res_id in self.RES_IDS.items():
            mbo = soort_ow == 'mbo'
            gezag = 'gezag' in soort_ow
            url_query = ''
            url_qry_sql = f'SELECT * FROM "{res_id}" WHERE '
            op = '='

            if naam:
                naam = naam.strip().replace('&', 'en')
                if mbo:
                    url_query = f'"INSTELLINGSNAAM":"{naam}"'
                    url_qry_sql += f"\"INSTELLINGSNAAM\" {op} '{naam}'"
                elif gezag:
                    url_query = f'"BEVOEGD GEZAG NAAM":"{naam}"'
                    url_qry_sql += f"\"BEVOEGD GEZAG NAAM\" {op} '{naam}'"
                else:
                    url_query = f'"VESTIGINGSNAAM":"{naam}"'
                    url_qry_sql += f"\"VESTIGINGSNAAM\" {op} '{naam}'"

            if plaats:
                plaats = plaats.strip()
                if 'zuidoost' in plaats.lower():
                    plaats = 'amsterdam zuidoost'
                if url_query:
                    url_query += f', "PLAATSNAAM":"{plaats}"'
                else:
                    url_query = f'"PLAATSNAAM":"{plaats}"'

            if adres:
                straat, nr = re.findall(r'([a-zA-Z -.]+)([\w-]*)', adres)[0]
                straat, nr = straat.strip(), nr.strip()

                if '-' in nr:
                    nr = nr.split('-')[0]

                if url_query:
                    if 'postbus' in adres.lower():
                        url_query += f', "STRAATNAAM CORRESPONDENTIEADRES": "{straat}"'
                    else:
                        url_query += f', "STRAATNAAM": "{straat}"'
                        if nr:
                            url_query += f', "HUISNUMMER-TOEVOEGING": "{nr}"'
                else:
                    if 'postbus' in adres.lower():
                        url_query = f'"STRAATNAAM CORRESPONDENTIEADRES": "{straat}"'
                    else:
                        url_query = f'"STRAATNAAM": "{straat}"'
                        if nr:
                            url_query += f', "HUISNUMMER-TOEVOEGING": "{nr}"'

            if pd.notna(website) and website:
                from urllib.parse import urlparse
                if not website.startswith('http'):
                    website = f'http://{website}'
                website = urlparse(website).netloc.strip()
                if url_query:
                    url_query += f', "INTERNETADRES": "{website}"'
                else:
                    url_query = f'"INTERNETADRES": "{website}"'

            if brin4:
                if not gezag and not mbo:
                    url_query = f'"BRIN NUMMER": "{brin4}"'

            if brin6:
                if not gezag and not mbo:
                    url_query = f'"VESTIGINGSNUMMER": "{brin6}"'

            if url_query:
                urls[soort_ow] = f'{self.BASE_URL}{res_id}&q={{{url_query}}}'
                # print(url_query)

        return urls

    @staticmethod
    def brin_switch(soort_ow):
        if 'gezag' in soort_ow:
            naam_result = 'BEVOEGD GEZAG NAAM'
            nr_result = 'BEVOEGD GEZAG NUMMER'
        elif soort_ow == 'mbo':
            naam_result = 'INSTELLINGSNAAM'
            nr_result = 'BRIN NUMMER'
        else:
            nr_result = 'VESTIGINGSNUMMER'
            naam_result = 'VESTIGINGSNAAM'
        return nr_result, naam_result

    @staticmethod
    def pick_result(results: pd.DataFrame, display_cols: bool = True, pickfirst: bool = False):
        if pickfirst or len(results) == 1:
            print(f'{len(results)} result(s), picked first')
            return results.iloc[0]
        else:
            if display_cols:
                display_cols = [
                    'soort_ow',
                    'PLAATSNAAM',
                    'VESTIGINGSNAAM',
                    # 'BRIN NUMMER',
                    # 'VESTIGINGSNUMMER',
                    # 'BEVOEGD GEZAG NUMMER',
                    'STRAATNAAM',
                    'HUISNUMMER-TOEVOEGING',
                    "STRAATNAAM CORRESPONDENTIEADRES",
                    'INTERNETADRES'
                ]
                if 'VESTIGINGSNAAM' in results.columns:
                    display_cols += ['VESTIGINGSNAAM']
                # if 'mbo' in results.soort_ow.unique():
                #     display_cols += ['INSTELLINGSNAAM']
                if any(['gezag' in q for q in results.soort_ow.unique()]):
                    display_cols += ['BEVOEGD GEZAG NAAM']
                    display_cols.remove('VESTIGINGSNAAM')
                # print()
                print(results[display_cols].to_markdown())
            else:
                print(results.to_markdown())

            while True:
                try:
                    numr = str(input('kies resultaat id: '))
                    if numr.isdigit() and int(numr) in results.index:
                        break
                    else:
                        raise ValueError
                except ValueError:
                    print('probeer nog eens')
            return results.loc[int(numr)]

    @staticmethod
    def filter_values(df):
        df = df.loc[df['PLAATSNAAM'] != 'NIEUW-AMSTERDAM']
        return df

    def find_school(
            self,
            naam: str = None,
            plaats: str = None,
            website: str = None,
            adres: str = None,
            brin4: str = None,
            brin6:  str = None,
            all_fields: bool = True,
            transpose: bool = True,
            copy: bool = False,
            pickfirst: bool = False,
            retry_count: int = 0
    ) -> Optional[pd.DataFrame]:

        data = {}
        urls = self._build_urls(naam, plaats, website, brin4, brin6, adres)
        print(f'naam="{naam}", adres="{adres}", plaats="{plaats}", website="{website}"', end='\n')
        print('-'*100)

        for soort_ow, url in urls.items():
            data[soort_ow] = get_json_from_url(url, 'result', self.ENCODING).get('records', [])

        # error on no results
        if not any(list(data.values())):
            print('Geen resultaten gevonden.')
            if retry_count == 0:
                return self.find_school(naam=naam, plaats=plaats, adres=adres, retry_count=1, pickfirst=pickfirst)
            if retry_count == 1:
                return self.find_school(plaats=plaats, adres=adres, website=website, pickfirst=pickfirst, retry_count=2)
            elif retry_count == 2:
                return self.find_school(plaats=plaats, adres=adres, pickfirst=pickfirst, retry_count=3)
            if retry_count == 3:
                return self.find_school(naam=naam, pickfirst=pickfirst, retry_count=4)
            else:
                return None

        else:
            all_results = []
            for soort_ow, records in data.items():
                for rec in records:
                    rec['soort_ow'] = soort_ow
                    all_results.append(rec)

            all_results = pd.DataFrame.from_records(all_results).set_index('_id')
            all_results = self.filter_values(all_results)

            if len(all_results) > 20:
                pd.set_option('display.max_rows', len(all_results) + 1)
            result = self.pick_result(all_results, display_cols=True, pickfirst=pickfirst)

            if not all_fields:
                result = [
                    result['PLAATSNAAM'],
                    result['BRIN NUMMER'] if result['soort_ow'] == 'mbo' else result['VESTIGINGSNUMMER'],
                    result['INSTELLINGSNAAM'] if result['soort_ow'] == 'mbo' else result['VESTIGINGSNAAM'],
                    result['PLAATSNAAM'],
                    f'{result["STRAATNAAM"]} {result["HUISNUMMER-TOEVOEGING"]}',
                    result['INTERNETADRES']
                ]
                idx = ['PLAATS', 'BRIN NUMMER', 'INSTELLINGSNAAM', 'PLAATSNAAM', 'STRAATNAAM', 'INTERNETADRES']
                result = pd.DataFrame(result, index=idx)

            if copy:
                result.to_clipboard(index=False, header=False)
                print(' --- COPIED --- ')

            return result.T if transpose else result

    def find_brin(self, naam, plaats, website, adres, n: int = None, pickfirst: bool = False):
        if (
                isinstance(naam, pd.Series) and
                isinstance(plaats, pd.Series) and
                isinstance(website, pd.Series) and
                isinstance(adres, pd.Series)
        ):
            naam.name, plaats.name, website.name, adres.name = 'NAAM', 'PLAATS', 'WEBSITE', 'ADRES'
            df = pd.concat([naam, plaats, website, adres], axis=1)
            if n:
                df = df.iloc[:n]
            return df.join(
                df.apply(
                    lambda x: self.find_school(
                        naam=x[0],
                        plaats=x[1],
                        website=x[2],
                        adres=x[3],
                        pickfirst=pickfirst
                    ), axis=1,
                )
            )
        else:
            res = self.find_school(naam=naam, plaats=plaats, website=website, all_fields=True, pickfirst=pickfirst)
            return f"{res['BRIN NUMMER']}, {res['VESTIGINGSNUMMER']}"
