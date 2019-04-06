''' module docstring, now shut up pylint'''

import os
import csv
import pandas as pd
import numpy as np
import geopandas as gpd


def load_engineered_data(ch_psc_path, ch_off_path, ch_accounts_path):
    ''' docstring here '''

    ch_psc = pd.read_csv(ch_psc_path, encoding='latin-1',
                         sep='\t', error_bad_lines=False,
                         dtype={'company_number': str,
                                'name': str,
                                'forename': str,
                                'title': str,
                                'nationality': str,
                                'postal_code': str,
                                'date_of_birth': str,
                                'gender': str,
                                'SICCode.SicText_1': str,
                                'SIC_INT': np.float64,
                                'SIMPLE_SIC': str,
                                'Age': np.float64,
                                'isfemale': np.float64,
                                'Country': str,
                                'Postcode': str,
                                'District': str,
                                'NUTS118NM': str,
                                'LSOA Code': str,
                                'Index of Multiple Deprivation': np.float64,
                                'nationality_cleaned': str,
                                'Postcode Area': str},
                         usecols=['company_number',
                                  'name',
                                  'nationality',
                                  'postal_code',
                                  'forename',
                                  'title',
                                  'isfemale',
                                  'date_of_birth',
                                  'gender',
                                  'Country',
                                  'SICCode.SicText_1',
                                  'SIC_INT',
                                  'SIMPLE_SIC',
                                  'Age',
                                  'Postcode',
                                  'District',
                                  'NUTS118NM',
                                  'LSOA Code',
                                  'Index of Multiple Deprivation',
                                  'nationality_cleaned',
                                  'Postcode Area'])
    ch_off = pd.read_csv(ch_off_path, encoding='latin-1',
                         sep='\t', error_bad_lines=False,
                         parse_dates=['Appointed_DT', 'Resigned_DT'],
                         dtype={'CompanyNumber': str,
                                'Name': str,
                                'forename': str,
                                'nationality': str,
                                'Officer Role': str,
                                'postal_code': str,
                                'Country': str,
                                'date_of_birth': str,
                                'Officer ID': str,
                                'gender': str,
                                'Occupation': str,
                                'isfemale': np.float64,
                                'SICCode.SicText_1': str,
                                'Appointed_Age': np.float64,
                                'Resigned_Age': np.float64,
                                'SIC_INT': np.float64,
                                'SIMPLE_SIC': str,
                                'Age': np.float64,
                                'Postcode': str,
                                'District': str,
                                'NUTS118NM': str,
                                'LSOA Code': str,
                                'Index of Multiple Deprivation': np.float64,
                                'nationality_cleaned': str,
                                'Postcode Area': str},
                         usecols=['CompanyNumber',
                                  'Appointed',
                                  'Resigned',
                                  'Appointed_Age',
                                  'Appointed_DT',
                                  'Resigned_DT',
                                  'Resigned_Age',
                                  'Name',
                                  'nationality',
                                  'Occupation',
                                  'postal_code',
                                  'Country',
                                  'forename',
                                  'Officer Role',
                                  'Officer ID',
                                  'date_of_birth',
                                  'isfemale',
                                  'gender',
                                  'SICCode.SicText_1',
                                  'SIC_INT',
                                  'SIMPLE_SIC',
                                  'NUTS118NM',
                                  'Age',
                                  'Postcode',
                                  'District',
                                  'LSOA Code',
                                  'Index of Multiple Deprivation',
                                  'nationality_cleaned',
                                  'Postcode Area'])
    ch_accounts = pd.read_csv(ch_accounts_path,
                              sep='\t',
                              compression='gzip',
                              dtype={'NetCurrentAssetsLiabilities': float,
                                     'TotalAssetsLessCurrentLiabilities': float,
                                     'CurrentAssets': float,
                                     'ch_num': str})
    return ch_psc, ch_off, ch_accounts


def load_raw_data(basicpath, pscpath, officerpath):
    ch_psc = pd.read_csv(pscpath, encoding='latin-1',
                         sep='\t', error_bad_lines=False,
                         warn_bad_lines=False,
                         engine='python',
                         dtype={'company_number': str,
                                'name': str,
                                'forename': str,
                                'title': str,
                                'nationality': str,
                                'postal_code': str,
                                'date_of_birth': str},
                         usecols=['company_number',
                                  'name',
                                  'nationality',
                                  'postal_code',
                                  'forename',
                                  'title',
                                  'date_of_birth'])
    ch_basic = pd.read_csv(basicpath,
                           encoding='latin-1', sep=',',
                           error_bad_lines=False,
                           warn_bad_lines=False,
                           dtype={' CompanyNumber': str,
                                  'SICCode.SicText_1': str},
                           usecols=[' CompanyNumber',
                                    'SICCode.SicText_1'])
    ch_off = pd.read_csv(officerpath,
                         encoding='latin-1', engine='python',
                         sep='\t', error_bad_lines=False,
                         warn_bad_lines=False,
                         dtype={'CompanyNumber': str,
                                'Appointed': str,
                                'Resigned': str,
                                'Name': str,
                                'Occupation': str,
                                'Officer ID': str,
                                'Nationality': str,
                                'postal_code': str,
                                'Date of Birth': str,
                                'Officer Role': str},
                         usecols=['CompanyNumber',
                                  'Name',
                                  'Date of Birth',
                                  'Nationality',
                                  'Occupation',
                                  'postal_code',
                                  'Officer ID',
                                  'Appointed',
                                  'Resigned',
                                  'Officer Role'])
    ch_basic['SIC_INT'] = ch_basic['SICCode.SicText_1'].str.extract('(\d+)')
    ch_basic['SIC_INT'] = pd.to_numeric(ch_basic['SIC_INT'])
    ch_basic['SIMPLE_SIC'] = ch_basic['SIC_INT'].map(lambda x: sicmaker(x))
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('85100 - Pre-primary education',
                     'Pre-primary Education')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('25730 - Manufacture of tools',
                     'Mfg Tools')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('86101 - Hospital activities',
                     'Hospital activites')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('88910 - Child day-care activities',
                     'Child Day-Care')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('96020 - Hairdressing and other beauty treatment',
                     'Hairdressing and Beauty')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('86102 - Medical nursing home activities',
                     'Medical Nursing Home')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('14132 - Manufacture of other women\'s outerwear',
                     'Mfg Women\'s Outerwear')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('29100 - Manufacture of motor vehicles',
                     'Mfg Motor Vehicles')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('22220 - Manufacture of plastic packing goods',
                     'Mfg Plastic Packaging')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('49410 - Freight transport by road',
                     'Freight Transport (Road)')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('53201 - Licensed carriers',
                     'Licensed Carriers')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('33160 - Repair and maintenance of aircraft and spacecraft',
                     'Repair/Maintain Air/Space')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('84120 - Regulation of health care, education, cultural and other social services, not incl. social security',
                     'Regulating Social Services')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('35140 - Trade of electricity',
                     'Trade of Electricity')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str\
        .replace('42120 - Construction of railways and underground railways',
                 'Construction of Railways')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('58210 - Publishing of computer games',
                     'Computer Game Publishing')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('47421 - Retail sale of mobile telephones',
                     'Mobile Phone Sales')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('14131 - Manufacture of other men\'s outerwear',
                     'Mfg Men\'s Outerwear')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('52103 - Operation of warehousing and storage facilities for land transport activities',
                     'Storage for Land Transport')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('64191 - Banks	',
                     'Banks')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('28290 - Manufacture of other general-purpose machinery n.e.c.',
                     'General Purpose Machinery')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('01500 - Mixed farming',
                     'Mixed Farming')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('26511 - Manufacture of electronic measuring, testing etc. equipment, not for industrial process control',
                     'Mfg Electronic Measuring')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('47782 - Retail sale by opticians',
                     'Opticians Sales')
    ch_basic['SICCode.SicText_1'] = ch_basic['SICCode.SicText_1'] \
        .str.replace('01110 - Growing of cereals \(except rice\), leguminous crops and oil seeds',
                     'Cereal, Crop and Oil Seeds')
    return ch_basic, ch_psc, ch_off


def load_nat(path):
    ''' load nationalities files for wrangling'''

    with open(os.path.abspath(os.path.join(path,
                                           'list_to_remove.txt')),
              'r') as file_in:
        anom_remove = file_in.read().splitlines()
    anom_replace = pd.read_csv(os.path.abspath(
                               os.path.join(path,
                                            'nationality_dictionary.csv')),
                               header=None,
                               index_col=0,
                               squeeze=True).to_dict()
    un_members = []
    with open(os.path.abspath(
              os.path.join(path,
                           'list_of_un_member_states.csv')),
              newline='') as inputfile:
        for row in csv.reader(inputfile):
            un_members.append(row[0])
    ons_df = pd.read_csv(os.path.abspath(
                         os.path.join(path,
                                      'ons_thousands_in_uk.csv')))
    ons_df['uk_pop'] = ons_df['uk_pop'] * 1000
    return anom_remove, anom_replace, un_members, ons_df


def load_token(token_path):
    try:
        with open(token_path, 'r') as file:
            return str(file.readline()).strip()
    except EnvironmentError:
        print('Error loading access token from file')


def sicmaker(sic_int):
    if (sic_int >= 1110) & (sic_int <= 3220):
        return 'Agriculture, Forestry, Fishing'
    elif (sic_int >= 5501) & (sic_int <= 9900):
        return 'Mining'
    elif (sic_int >= 10110) & (sic_int <= 33200):
        return 'Manufacturing'
    elif (sic_int >= 35110) & (sic_int <= 35300):
        return 'EFGSAC Supply'
    elif (sic_int >= 36000) & (sic_int <= 39000):
        return 'WSWMR'
    elif (sic_int >= 41100) & (sic_int <= 43999):
        return 'Construction'
    elif (sic_int >= 45111) & (sic_int <= 47990):
        return 'Wholesale Trade'
    elif (sic_int >= 49100) & (sic_int <= 53202):
        return 'Transportation & Storage'
    elif (sic_int >= 55100) & (sic_int <= 56302):
        return 'Accomodation & Food Service'
    elif (sic_int >= 58110) & (sic_int <= 63990):
        return 'Information & Communication'
    elif (sic_int >= 64110) & (sic_int <= 66300):
        return 'Fire & Insurance'
    elif (sic_int >= 68100) & (sic_int <= 68320):
        return 'Real Estate'
    elif (sic_int >= 69101) & (sic_int <= 75000):
        return 'PST'
    elif (sic_int >= 77110) & (sic_int <= 82990):
        return 'Administration & Support'
    elif (sic_int >= 84110) & (sic_int <= 84300):
        return 'Public Admin & Defense'
    elif (sic_int >= 86101) & (sic_int <= 88990):
        return 'Health & Social'
    elif (sic_int >= 90010) & (sic_int <= 93290):
        return 'Arts, Ent & Rec'
    elif (sic_int >= 94110) & (sic_int <= 96090):
        return 'Other Service'
    elif (sic_int >= 97000) & (sic_int <= 98200):
        return 'Households as Employers'
    elif (sic_int >= 99000) & (sic_int <= 99999):
        return 'Extraterritorial'


def load_regional(Regional):
    postcodes = pd.read_csv(os.path.join(Regional,
                                         'doogal',
                                         'postcodes.csv'),
                            dtype={'Postcode': str,
                                   'Index of Multiple Deprivation': str,
                                   'LSOA Code': str,
                                   'Country': str,
                                   'District': str},
                            usecols=['Postcode',
                                     'LSOA Code',
                                     'Country',
                                     'Index of Multiple Deprivation',
                                     'District'])
    map_london_df = gpd.read_file(os.path.join(
        Regional,
        'shapefiles',
        'statistical-gis-boundaries-london',
        'ESRI',
        'London_Borough_Excluding_MHW.shp'))
    map_uk_df = gpd.read_file(os.path.join(Regional,
                                           'shapefiles',
                                           'distribution',
                                           'Areas.shp'))
    map_uk_df = map_uk_df.to_crs({'init': 'epsg:27700'})

    dist_to_nuts = pd.read_csv(os.path.join(
        Regional,
        'district_to_nuts.csv'))
    dist_to_nuts['LAD16NM'] = dist_to_nuts['LAD16NM'].str\
        .replace('Shepway',
                 'Folkestone and Hythe')
    dist_to_nuts = dist_to_nuts.drop_duplicates(subset='LAD16NM', keep=False)
    return postcodes, map_london_df, map_uk_df, dist_to_nuts
