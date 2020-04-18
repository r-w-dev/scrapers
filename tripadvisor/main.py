"""
Main running class.

@author: Roel de Vries
@email: roel.de.vries@amsterdam.nl
"""
import os
import pickle
import signal
from datetime import datetime

import pandas as pd
from more_itertools import flatten
from selenium.common.exceptions import NoSuchElementException

from browser import Browser
from scrape_1 import get_categories
from scrape_2 import get_activities
from scrape_3 import Attractie


def _datetime_now() -> str:
    return datetime.now().strftime('%d-%m-%Y %H%M')


def create_dataframe(cat_dump: str, act_dump: str, attrs_dump: str) -> pd.DataFrame:
    """Create dataframe from scraped data."""
    with open(cat_dump, 'rb') as s1:
        cats = pickle.load(s1)

    with open(act_dump, 'rb') as s2:
        acts = pickle.load(s2)

    with open(attrs_dump, 'rb') as s3:
        attrs = pickle.load(s3)

    df_cats = pd.DataFrame(cats, columns=[
        'categorie',
        'cat_url',
        'added',
        'status',
        'provincie'
    ])

    df_acts = pd.DataFrame(acts, columns=[
        'titel',
        'attrac_url',
        'added',
        'status',
        'provincie',
        'cat_url'
    ])

    df_attr = pd.DataFrame(
        attrs,
        columns=[
            'status',
            'titel',
            'attrac_url',
            'ta_id',
            'beoordeling',
            'adres',
            'postcode',
            'plaats',
            'land',
            'aantal_reviews',
            'percentage_excellent',
            'percentage_verygood',
            'percentage_average',
            'percentage_poor',
            'percentage_terrible',
            'lat',
            'lon'
        ],
    )

    res = df_attr \
        .assign(**{
            'attrac_url': df_attr['attrac_url'].apply(lambda x: f'/{x}' if not x.startswith('/') else x)
        }) \
        .merge(df_acts, how='left', on='attrac_url') \
        .merge(df_cats, how='left', on='cat_url')

    assert res.isna().sum().sum() == 0

    res = res \
        .drop(columns=[c for c in res.columns if c.endswith('_y')] + ['status', 'cat_url']) \
        .rename(columns={c: c.rstrip('_x') for c in res.columns if c.endswith('_x')}) \
        .astype({'added': 'datetime64', 'categorie': 'category'}) \

    res['added'] = res['added'].apply(lambda x: x.strftime('%d-%m-%Y'))
    return res


def write_to_csv(data: pd.DataFrame):
    """Write csv file to disk."""
    print('writing csv for backup...')
    try:
        time = _datetime_now()
        data.to_csv('results/attracties {0}.csv'.format(time), sep=';', index=False)
    except Exception as e_:
        print(e_.__class__)
        print('writing csv failed...')


def pivot_categories(data: pd.DataFrame) -> pd.DataFrame:
    """Pivot categorie kolom in dataframe."""
    pivot_cat = data \
        .loc[:, ['provincie', 'attrac_url', 'categorie']] \
        .pivot_table(
            columns='categorie',
            index=['provincie', 'attrac_url'],
            aggfunc='size'
        )

    gby_cols = [
        'provincie',
        'attrac_url',
        'status',
        'titel',
        'beoordeling',
        'adres',
        'postcode',
        'plaats',
        'land',
        'added'
    ]
    agg_cols = [
        'aantal_reviews',
        'percentage_excellent',
        'percentage_verygood',
        'percentage_average',
        'percentage_poor',
        'percentage_terrible',
        'lat',
        'lon'
    ]

    res = data \
        .drop(columns=['categorie']) \
        .groupby(gby_cols, as_index=False)[agg_cols] \
        .max() \
        .set_index(['provincie', 'attrac_url'], verify_integrity=True) \
        .sort_index()

    return res.join(pivot_cat).reset_index()


def running_time(start):
    now = datetime.now()
    print('\nrunning: {0:.0f} minute(s)...\n'.format(
        round((now - start).total_seconds() / 60, 0)))


def print_item(idx: int, links, ta_link: str):
    print('[{0}/{1}] {2}'.format(idx + 1, len(links), ta_link))


def _sqlcol(dfparam):
    from sqlalchemy import Numeric, Date, String, Integer
    dtypedict = {}
    for c, d in zip(dfparam.columns, dfparam.dtypes):
        if 'object' in str(d):
            dtypedict.update({c: String()})
        if 'datetime' in str(d):
            dtypedict.update({c: Date()})
        if 'float' in str(d):
            dtypedict.update({c: Numeric()})
        if 'int' in str(d):
            dtypedict.update({c: Integer()})
        if c == 'lat' or c == 'lon' or c == 'beoordeling':
            dtypedict.update({c: Numeric()})

    return dtypedict


def write_to_db(s1: list, s2: list, s3: set, data: pd.DataFrame) -> None:
    """Write lists with data to database."""
    from .psql import Psql
    from .Bases import Attractie, Activity, Categorie, SCHEMA

    db = Psql(SCHEMA).set_sess_maker().set_engine(echo=False)
    sessie = db.sessie_maker()
    engine = db.engine

    try:
        sessie.add_all([Categorie(*c) for c in s1])
        sessie.add_all([Activity(*a) for a in s2])
        sessie.add_all([Attractie(*a) for a in s3])
        sessie.commit()

    except Exception as ex:
        print(ex.__class__)
        sessie.rollback()
        raise

    finally:
        sessie.close()

    try:
        dtypes = _sqlcol(data)
        data.to_sql(
            'Combined',
            engine,
            schema=SCHEMA,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000,
            dtype=dtypes
        )

    except ValueError as e_:
        print(e_)
        print('FOUT: toevoegen combined mislukt.')


def print_update(begin_, aantal, type_):
    running_time(begin_)
    print(aantal, type_)


def init_browser(base_url: str, headless: bool):
    from scrape_2 import _wait_for
    chrome = Browser(base_url, headless=headless)

    # klik op continue om op tripadvisor.com te blijven
    try:
        cont = "//span[@class='continue']"
        _wait_for(chrome.driver, cont)
        chrome.driver.find_element_by_xpath(cont).click()

    except NoSuchElementException:
        print("Continue niet gevonden. (al op tripadvisor.com)")

    return chrome


def dump_to_file(to_dump: list):
    with open(f'activities {_datetime_now()}.txt', 'w') as f:
        f.writelines(f'{a[1]}\n' for a in to_dump)


def dump_to(file_name: str, to_dump: list):
    if to_dump:
        print(f'Dumping to "{file_name}" ({len(to_dump)})')

        with open(file_name, 'wb') as f:
            pickle.dump(to_dump, f)

    else:
        print(f"{file_name} bevat geen data.")


def handle_sig_term(signum, frame):
    print("handling sudden stop")
    signal.signal(signum, signal.SIG_IGN)

    print("  -- FINALLY --  ")
    dump_to(file_cat, categories)
    dump_to(file_act, activities)
    dump_to(file_att, attracties)
    import sys
    sys.exit(0)


if __name__ == '__main__':
    # signal.signal(signal.SIGINT, handle_sig_term)

    browser = None
    categories, activities, attracties = [], [], []

    begin = datetime.now()
    begin_fmt = begin.strftime('%d-%m-%Y %H%M')
    output = f'results/{begin.strftime("%Y%m%d")}'
    os.makedirs(output, exist_ok=True)
    
    file_cat = f'{output}/categories {begin_fmt}.pickle'
    file_act = f'{output}/activities {begin_fmt}.pickle'
    file_att = f'{output}/attracties {begin_fmt}.pickle'

    try:
        browser = init_browser('http://www.tripadvisor.com', headless=True)

        categories.extend(get_categories(browser))

        activities.extend(flatten(get_activities(cat, browser) for cat in categories))

        activ_links = {act[1] for act in activities}
        attracties.extend({Attractie(act_link).data for act_link in activ_links})

    except Exception as e:
        print(e.__class__)

    else:
        df = create_dataframe(file_cat, file_act, file_att)  
        df = pivot_categories(df)

        df.to_pickle(f'result {begin}.pickle')
        write_to_csv(df)
        # write_to_db(scrape1, scrape2, scrape3, df)

    finally:
        print("  -- FINALLY --  ")
        dump_to(file_cat, categories)
        dump_to(file_act, activities)
        dump_to(file_att, attracties)

        if browser:
            browser.kill()

        running_time(begin)
