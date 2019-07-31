import re

import numpy as np
import pandas as pd
import os
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def housenr(x):
    # This function takes the adress column from the dataframe and strips
    # the streetname (in street column) from and returns it.
    return x['adres'].lstrip(x['straat'])


def read_df(path):
    cols = ['Postcode', 'Address', 'Straat', 'City', 'Aangeboden sinds', 'Soort woning', 'Status', 'Oppervlakte (m²)',
            'Perceeloppervlakte (m²)', 'Aantal kamers', 'Aantal slaapkamers', 'Bouwjaar', 'Inhoud  (m³) ',
            'Tuin aanwezig', 'Vraagprijs', ' Energielabel ', 'Verwarming', 'Isolatie', 'Source', 'Date']

    df = pd.read_json(path, lines=True)

    df = df[cols].rename(columns={'Address': 'adres',
                                  'City': 'plaats',
                                  'Oppervlakte (m²)': 'oppervlakte',
                                  'Perceeloppervlakte (m²)': 'perceeloppervlakte',
                                  'Inhoud  (m³) ': 'inhoud',
                                  'Tuin aanwezig': 'tuin',
                                  'Vraagprijs': 'huidige_vraagprijs',
                                  ' Energielabel ': 'energielabel',
                                  'Source': 'bron',
                                  'Date': 'laatste_scrape',
                                  'Postcode': 'postcode',
                                  'Straat': 'straat',
                                  'Aangeboden sinds': 'aangeboden_sinds',
                                  'Soort woning': 'soort_woning',
                                  'Status': 'status',
                                  'Aantal kamers': 'aantal_kamers',
                                  'Aantal slaapkamers': 'aantal_slaapkamers',
                                  'Bouwjaar': 'bouwjaar',
                                  'Verwarming': 'verwarming',
                                  'Isolatie': 'isolatie'})

    return df


def read_bag(path):
    # Reading BAG dataset
    bag_df = pd.read_csv(path, dtype=object, header=None,
                         names=['straat_BAG', 'huisnummer_BAG',
                                'huisletter_BAG', 'toevoeging_BAG',
                                'postcode_BAG', 'plaats_BAG',
                                'gemeente_BAG', 'provincie_BAG'])

    # Creating an index_BAG column fromthe index
    bag_df = bag_df.reset_index()
    bag_df = bag_df.rename(columns={'index': 'index_BAG'})

    # Convert all house letters in the BAG dataframe to uppercase
    bag_df['huisletter_BAG'] = bag_df['huisletter_BAG'].str.upper()

    bag_df['toevoeging_BAG'] = bag_df['toevoeging_BAG'].fillna('NULL')
    bag_df['huisletter_BAG'] = bag_df['huisletter_BAG'].fillna('NULL')

    # remove duplicates from bag_df
    bag_df.drop_duplicates(subset=['postcode_BAG', 'huisnummer_BAG', 'toevoeging_BAG', 'huisletter_BAG'],
                           inplace=True)
    return bag_df


def clean_fields(df):
    # huidige vraagprijs
    df['huidige_vraagprijs'] = df['huidige_vraagprijs'].apply(
        lambda x: pd.to_numeric(str(x).strip('€ ,-').replace('.', ''), downcast='float'))

    # oppervlakte en inhoud
    df['oppervlakte'] = df['oppervlakte'].str.rstrip(' m²')
    df['inhoud'] = df['inhoud'].str.rstrip(' m³')
    
    # converting perceeloppervlakte to float
    df['perceeloppervlakte'] = df['perceeloppervlakte'].astype(str).str.replace('.','')
    df['perceeloppervlakte'] = df['perceeloppervlakte'].astype(str).str.strip(' m²')
    df['perceeloppervlakte'] = pd.to_numeric(df['perceeloppervlakte'], errors='coerce')

    # plaats
    df['plaats'] = df['plaats'].apply(lambda x: str(x.lstrip('in ')))

    # Splitting of the house number and suffix as described in chapter 5.4.1 in the documentation
    df['huisnummer'] = df.apply(housenr, axis=1)
    df[['huisnummer', 'toevoeging']] = df['huisnummer'].str.split('-', n=1, expand=True)

    # postcode
    df['postcode'] = df['postcode'].str.replace(' ', '')

    # split huisletter and toevoeging
    regex = re.compile(r'(?P<huisletter>[A-Z]*)-*(?P<toevoeging>\d+)')
    df[['huisletter', 'toevoeging']] = df['toevoeging'].str.extract(regex, expand=True).replace('', 'NaN')

    # Replace house letter 'NO' (which stands for number) with nan
    df['huisletter'] = df['huisletter'].replace('NO', np.nan)

    return df


def match(df, bag_df):
    # Replacing all nan's in the columns 'toevoeging' and 'huisletter'
    # of both dataframes with NULL, necessary for merging
    df['toevoeging'] = df['toevoeging'].fillna('NULL')
    df['huisletter'] = df['huisletter'].fillna('NULL')
    df['huisletter'] = df['huisletter'].str.replace('NaN', 'NULL')

    # Left merging Master (on the left) with BAG (right)
    merge1 = pd.merge(left=df,
                      right=bag_df,
                      how='left',
                      left_on=['postcode', 'huisnummer', 'toevoeging', 'huisletter'],
                      right_on=['postcode_BAG', 'huisnummer_BAG', 'toevoeging_BAG', 'huisletter_BAG'])

    # Extracting all rows from master_merge dataframe which have Nan values in index_BAG column
    merge2 = merge1.loc[merge1['index_BAG'].isnull()].reset_index()

    # Defining the columns that will be merged the BAG dataframe
    merge_cols = ['straat_BAG', 'huisnummer_BAG', 'postcode_BAG', 'plaats_BAG', 'gemeente_BAG', 'provincie_BAG']

    # Left merging with null_BAG_index dataframe on left and BAG dataframe on the right.
    # The index of null_BAG_index is preserved.
    # In this merging the rows that failed to match on index_BAG are now matched on postal code and house number
    merge2 = pd.merge(left=merge2.drop(columns=list(bag_df)),
                      right=bag_df[merge_cols].drop_duplicates(subset=['postcode_BAG', 'huisnummer_BAG']),
                      how='left',
                      left_on=['postcode', 'huisnummer'],
                      right_on=['postcode_BAG', 'huisnummer_BAG']).set_index('index')

    # Creating a list which contains the columns used for updating the master_merge dataframe
    update_columns = list(merge2)[23:]

    # Updating the master_merge dataframe with the null_BAG_index dataframe.
    # Rows of the master_merge which have failed to completely match with BAG are
    # now updated from null_BAG_index df which matched on house number and postal code
    merge1.update(merge2[update_columns])

    return merge1


def add_cols(df):
    # Defining conditions and choices for matched_BAG columns:
    # 2 for complete match, 1 for partial match and 0 for no match.
    conditions = [
        (df['index_BAG'].notnull()),
        (df['index_BAG'].isnull()) & (df['postcode_BAG'].notnull()),
        (df['postcode_BAG'].isnull())]

    choices = [2, 1, 0]
    df['matched_BAG'] = np.select(conditions, choices)

    # Adding columns for compatability which are not in www.zah.nl dataset but are jaap.nl dataset
    df = pd.concat([df, pd.DataFrame(columns=['oorspr_vraagprijs', 'prijs_wijzigingen_data',
                                              'prijs_per_m2',
                                              'tijd_in_de_verkoop', 'garage'])],
                   sort=True)

    df['matched_BAG'] = df['matched_BAG'].astype('int')

    return df


def convert_cols(df):
    cols_orded = ['straat', 'huisnummer', 'huisletter', 'toevoeging', 'postcode', 'plaats', 'huidige_vraagprijs',
                  'oorspr_vraagprijs', 'prijs_wijzigingen_data', 'prijs_per_m2',
                  'aangeboden_sinds', 'tijd_in_de_verkoop', 'status', 'soort_woning', 'bouwjaar', 'oppervlakte',
                  'perceeloppervlakte', 'aantal_kamers', 'aantal_slaapkamers', 'inhoud', 'tuin', 'garage',
                  'energielabel', 'verwarming', 'isolatie', 'bron', 'laatste_scrape', 'matched_BAG', 'index_BAG', 'straat_BAG',
                  'huisnummer_BAG', 'huisletter_BAG', 'toevoeging_BAG', 'postcode_BAG', 'plaats_BAG', 'gemeente_BAG',
                  'provincie_BAG']

    # Putting columns in correct oreder
    df = df[cols_orded]

    # Converting 'laatste_scrape' column from yyyy/mm/dd to dd-mm-yyyy
    df['laatste_scrape'] = pd.to_datetime(df['laatste_scrape']).dt.strftime('%d-%m-%Y')

    # Likewise with 'aangeboden_sinds' columns.
    df['aangeboden_sinds'] = df['aangeboden_sinds'].apply(
        lambda x: pd.to_datetime(x).strftime('%d-%m-%Y') if '>' not in x else '')

    # Replacing 'NULL' values with NaN's
    df.replace({'huisletter': {'NULL': ''}, 'toevoeging': {'NULL': ''},
                'huisletter_BAG': {'NULL': ''}, 'toevoeging_BAG': {'NULL': ''}}, inplace=True)

    return df


def scrape():
    # if not isinstance(start_url_list, list):
    #    raise TypeError('start_urls is not a list')
    #
    settings = get_project_settings()
    settings['FEED_URI'] = 'output_2.jl'
    settings['FEED_FORMAT'] = 'jsonlines'
    settings['LOG_LEVEL'] = 'INFO'
    settings['LOG_STDOUT'] = True
    # settings['LOG_FILE'] = 'output_log.txt'
    process = CrawlerProcess(settings)
    process.crawl('zoekallehuizen')
    process.start()


def main():
    scrape()

    df = read_df('output_2.jl')
    df = clean_fields(df)
    bag_df = read_bag('../../../bag_db/BAG_most_current.csv')
    df = match(df, bag_df)
    df = add_cols(df)
    df = convert_cols(df)
    os.remove('output_2.jl')
    filename = '../../../output/zah_master.csv'
    df.to_csv(filename, index=False)


main()
