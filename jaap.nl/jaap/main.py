import pandas as pd
import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def match(df, bag_df):
    # first try merging bag and jaap data on postcode, huisnummer, huisletter, toegoeging
    df_bag1 = df.merge(bag_df,
                       left_on=['postcode', 'huisnummer', 'huisletter', 'toevoeging'],
                       right_on=['postcode_BAG', 'huisnummer_BAG', 'huisletter_BAG', 'toevoeging_BAG'],
                       how='left'
                       ).drop_duplicates('jaap_id', keep='first')

    # give the rows that matched, value: 2. Put those values in column 'matched_BAG'
    df_bag1.insert(28, 'matched_BAG', df_bag1['index_BAG'].apply(lambda i: 2 if pd.notna(i) else None))

    # take the rows that didn't match a bag object
    df_bag2 = df_bag1.loc[df_bag1['index_BAG'].isna()].drop(bag_df.columns, axis=1)

    # second try merging only on postcode and huisnummer, don't take huisletter, toevoeging and index.
    df_bag2 = df_bag2.merge(bag_df.drop(columns=['huisletter_BAG', 'toevoeging_BAG', 'index_BAG']),
                            left_on=['postcode', 'huisnummer'],
                            right_on=['postcode_BAG', 'huisnummer_BAG'],
                            how='left'
                            ).drop_duplicates('jaap_id', keep='last')

    # give the rows that matched the second time, value: 1. Those who didn't match, value: 0
    df_bag2['matched_BAG'] = df_bag2['postcode_BAG'].apply(lambda i: 1 if pd.notna(i) else 0)

    # set index in both dataframes to 'jaap_id'
    df_bag1.set_index('jaap_id', inplace=True)
    df_bag2.set_index('jaap_id', inplace=True)

    # update the values in the first dataframe with the second-matched values
    df_bag1.update(df_bag2[['straat_BAG', 'postcode_BAG', 'huisnummer_BAG', 'matched_BAG',
                            'plaats_BAG', 'gemeente_BAG', 'provincie_BAG']],
                   overwrite=False)

    # reset and drop column 'jaap_id' (not in master db)
    df_bag1.reset_index(inplace=True, drop=True)

    # replace the -NULL- values back to ''
    df_bag1.replace({'huisletter': {'-NULL-': ''}, 'toevoeging': {'-NULL-': ''},
                     'huisletter_BAG': {'-NULL-': ''}, 'toevoeging_BAG': {'-NULL-': ''}}, inplace=True)

    # convert 'matched_BAG' column to int
    df_bag1['matched_BAG'] = df_bag1['matched_BAG'].astype('int')

    return df_bag1


def read_bag(path):
    bag_cols = ['straat_BAG', 'huisnummer_BAG', 'huisletter_BAG',
                'toevoeging_BAG', 'postcode_BAG', 'plaats_BAG',
                'gemeente_BAG', 'provincie_BAG']
    dtype = {'straat_BAG':      'str',
             'huisnummer_BAG':  'int',
             'huisletter_BAG':  'str',
             'toevoeging_BAG':  'str',
             'postcode_BAG':    'str',
             'plaats_BAG':      'str',
             'gemeente_BAG':    'str',
             'provincie_BAG':   'str'}

    bag = pd.read_csv(path, header=None, names=bag_cols, dtype=dtype, engine='c', encoding='windows-1251').reset_index()

    # lowercase huisletter and toevoeging, set to -NULL- otherwise (for merging)
    bag['huisletter_BAG'] = bag['huisletter_BAG'].apply(lambda h: str(h).lower() if pd.notna(h) else '-NULL-')
    bag['toevoeging_BAG'] = bag['toevoeging_BAG'].apply(lambda h: str(h).lower() if pd.notna(h) else '-NULL-')

    # rename column to match *_BAG and drop some duplicates
    bag.rename(columns={'index': 'index_BAG'}, inplace=True)
    bag.drop_duplicates(subset=['postcode_BAG', 'huisnummer_BAG', 'huisletter_BAG', 'toevoeging_BAG'],
                        keep='first', inplace=True)
    return bag


def read_jaap(path):
    dtype = {
        'straat':                       'str',
        'status':                       'str',
        'huisnummer':                   'int',
        'huisletter':                   'str',
        'toevoeging':                   'str',
        'postcode':                     'str',
        'plaats':                       'str',
        'provincie':                    'str',
        'huidige_vraagprijs':           'float',
        'oorspr_vraagprijs':            'float',
        'prijs_wijzigingen_data':       'str',
        'prijs_per_m2':                 'float',
        'tijd_in_de_verkoop':           'str',
        'bouwjaar':                     'float',
        'oppervlakte':                  'float',
        'perceeloppervlakte':           'float',
        'inhoud':                       'float',
        'aantal_kamers':                'float',
        'aantal_slaapkamers':           'float',
        'bijzonderheden':               'str',
        'isolatie':                     'str',
        'verwarming':                   'str',
        'jaap_id':                      'int'
    }
    jaap_df = pd.read_csv(path, encoding='windows-1251', engine='c', dtype=dtype)

    # fill missing values in huisletter and toevoeging columns with -NULL- (for merging)
    jaap_df.fillna({'huisletter': '-NULL-', 'toevoeging': '-NULL-'}, inplace=True)

    # convert 2 lists with data and prices to one dict: {date: price}
    jaap_df['prijs_wijzigingen_data'] = jaap_df['prijs_wijzigingen_data'].apply(split_prijs_wijzigingen)

    return jaap_df


def split_prijs_wijzigingen(data):
    if pd.notna(data):
        data = [i.split('/') for i in data.split('|')]
        return {data[0][i]: data[1][i] for i in range(len(data[0]))}
    else:
        return data


def scrape(start_url_list, filename='output', output_format='csv', logfile=False):
    if not isinstance(start_url_list, list):
        raise TypeError('start_urls is not a list')

    settings = get_project_settings()
    settings['FEED_URI'] = filename
    settings['FEED_FORMAT'] = output_format
    settings['LOG_LEVEL'] = 'INFO'
    settings['LOG_STDOUT'] = True

    if logfile:
        settings['LOG_FILE'] = filename[:-4] + '_log.txt'

    process = CrawlerProcess(settings)
    process.crawl('jaap', start_url_list)
    process.start()


def print_non_match(df):
    print('Dropped non-matched items: {:.2f}%'.format(len(df.loc[df['matched_BAG'] == 0])/len(df) * 100))
    print('Number of non-matched items: {}'.format(len(df.loc[df['matched_BAG'] == 0])))


def print_dupli(df):
    print('Dropped duplicate-matched items: {:.2f}%'.format(
        len(df.loc[df.duplicated(['postcode', 'huisnummer', 'huisletter', 'toevoeging'], keep='first')]) /
        len(df) * 100))
    print('Number duplicate-matched items: {}'.format(
        len(df.loc[df.duplicated(['postcode', 'huisnummer', 'huisletter', 'toevoeging'], keep='first')])))
    print(df.loc[df.duplicated(['postcode', 'huisnummer', 'huisletter', 'toevoeging'], keep=False)])


def path_exist(path):
    if os.path.exists(path):
        raise FileExistsError('file: ' + path + ' already exists')
    else:
        return path


def bag_path_not_exist(path):
    if not os.path.exists(path):
        raise FileExistsError('BAG database not present at: ' + path)
    else:
        return path


def main():
    provinces = ['drenthe', 'flevoland', 'friesland', 'gelderland', 'groningen', 'limburg',
                 'noord+brabant', 'noord+holland', 'overijssel', 'utrecht', 'zeeland', 'zuid+holland']

    output_format = 'csv'
    filename = path_exist('../../output/jaap_master_test.csv')
    bag_path = bag_path_not_exist('../../bag_db/BAG_most_current.csv')

    scrape_paths_list = ['noord+holland/groot-amsterdam/aalsmeer/aalsmeerderweg',
                         'friesland/noord-friesland/midsland',
                         'noord+holland/groot-amsterdam/amsterdam/sort7/p54']

    scrape(provinces, filename, output_format, logfile=True)

    bag_df = read_bag(bag_path)
    jaap_df = read_jaap(filename)

    output_df = match(jaap_df, bag_df)

    # print_non_match(output_df)

    print_dupli(output_df)
    output_df.drop_duplicates(subset=['postcode', 'huisnummer', 'huisletter', 'toevoeging'],
                              keep='first', inplace=True)

    os.remove(filename)
    output_df.to_csv(filename, encoding='windows-1251', index=False)
    

main()
