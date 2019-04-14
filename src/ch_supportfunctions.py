import json
from tqdm import tqdm
import os
import csv
import pandas as pd
from ratelimit import limits, sleep_and_retry
from reconcile import load_token
import requests
from requests.auth import HTTPBasicAuth
from time import gmtime, strftime
import base64
import hashlib

@sleep_and_retry
@limits(calls=600, period=600)
def call_people(urltocall, logfilehandler, APIKey, pars=None):
    ''' call people apis in a way that can be rate limited'''

    call_people.counter += 1
    people = requests.get(urltocall,
                          auth=HTTPBasicAuth(APIKey, ''),
                          params=pars)
    with open(logfilehandler, 'a') as f:
        f.write(str(format(people.status_code)) + ': ' +
                str(call_people.counter) + ': ' + urltocall +
                ': ' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '\n')
    return people


def ch_officers(id, APIKey):
    ''' master function for calling the officers api, appending the jsons
    into a list across various pages
    '''

    jsonlist = []
    CH = 'https://api.companieshouse.gov.uk/company/'
    pages = 1
    pars = {'items_per_page': '100',
            'start_index': str(((pages - 1) * 100))}
    url_id = CH + str(id) + '/officers'
    officers = call_people(url_id, os.path.abspath(
        os.path.join(ch_data_path, 'full_ch_officers.log')),
                     APIKey, pars)
    if officers.status_code == 200:
        jsonlist.append(officers.json())
        try:
            numberofficers = officers.json()['total_results']
            while pages < (numberofficers / 100):
                pages += 1
                pars = {'items_per_page': '100',
                        'start_index': str(((pages - 1) * 100))}
                officers = call_people(url_id, os.path.abspath(
                    os.path.join(ch_data_path, 'full_ch_officers.log')),
                                 APIKey, pars)
                jsonlist.append(officers.json())
        except KeyError:
            print('Whats going on with ' + str(id))
    return jsonlist


def make_psc_flatfile():
    with open(os.path.abspath(os.path.join(ch_data_path, 'psc_flatfile.tsv')),
              'w') as tsvfile:
        psc_data = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')
        psc_data.writerow(['company_number',
                           'country_of_residence',
                           'date_of_birth',
                           'nationality'])
        with open(os.path.abspath(
                  os.path.join(ch_data_path,
                               'persons-with-significant-control-snapshot-2018-09-07.txt')),
                  'r', encoding='latin1') as f:
            for line in tqdm(f):
                try:
                    company_number = json.loads(line)['company_number']
                except Exception as e:
                    company_number = 'N/A'
                try:
                    country_of_residence = json.loads(
                        line)['data']['country_of_residence']
                except Exception as e:
                    country_of_residence = 'N/A'
                try:
                    date_of_birth = json.loads(line)['data']['date_of_birth']
                except Exception as e:
                    date_of_birth = 'N/A'
                try:
                    nationality = json.loads(line)['data']['nationality']
                except Exception as e:
                    nationality = 'N/A'
                psc_data.writerow([company_number, country_of_residence,
                                   str(date_of_birth), nationality])


def scrape_full_officer_database(APIKey):
    bulk_ch = pd.read_csv(os.path.abspath(
        os.path.join(ch_data_path,
                     'BasicCompanyDataAsOneFile-2019-01-01.csv.gz')), sep=',',
        usecols=[' CompanyNumber'])
    if os.path.exists(os.path.abspath(
        os.path.join(__file__, '../..', 'data',
                     'companies_house',
                     'ch_full_officers.tsv'))) is False:
        with open(os.path.abspath(
            os.path.join(__file__, '../..', 'data',
                         'companies_house',
                         'ch_full_officers.tsv')), 'w') as tsvfile:
            officers_data = csv.writer(tsvfile, delimiter='\t',
                                       lineterminator='\n')
            officers_data.writerow(['CompanyNumber', 'New_ID',
                                    'Appointed', 'Resigned',
                                    'Date of Birth',
                                    'Country of Residence',
                                    'Nationality'])
        listofcompanies = bulk_ch[' CompanyNumber'].tolist()
    else:
        existing_flatfile = pd.read_csv(os.path.abspath(
            os.path.join(ch_data_path, 'ch_full_officers.tsv')),
            encoding='latin-1', sep='\t', error_bad_lines=False)
        listofcompanies = list(set(
            bulk_ch[' CompanyNumber'].tolist()) -
            set(
            existing_flatfile['CompanyNumber'].tolist()))

    for CompanyNumber in tqdm(listofcompanies):
        offjs = ch_officers(CompanyNumber, APIKey)
        if len(offjs) > 0:
            if offjs[0]['active_count'] > 0:
                for page in offjs:
                    for off in page['items']:
                        try:
                            New_ID = base64.\
                                     b64encode(hashlib.
                                               sha1(off['name'].
                                                    encode('UTF-8') +
                                                    str(off['date_of_birth']).encode('UTF-8')).digest())
                        except Exception as e:
                            New_ID = 'N/A'
                        try:
                            Appointed = off['appointed_on']
                        except Exception as e:
                            Appointed = 'N/A'
                        try:
                            Resigned = off['resigned_on']
                        except Exception as e:
                            Resigned = 'N/A'
                        try:
                            Nationality = off['nationality']
                        except Exception as e:
                            Nationality = 'N/A'
                        try:
                            Date_of_Birth = str(off['date_of_birth'])
                        except Exception as e:
                            Date_of_Birth = 'N/A'
                        try:
                            Country_of_Residence = off['country_of_residence']
                        except Exception as e:
                            Country_of_Residence = 'N/A'
                        with open(os.path.abspath(
                                  os.path.join(ch_data_path,
                                               'ch_full_officers.tsv')),
                                  'a', encoding='latin-1') as tsvfile:
                            officers_data = csv.writer(
                                tsvfile, delimiter='\t',
                                lineterminator='\n')
                            officers_data.writerow([CompanyNumber,
                                                    New_ID,
                                                    Appointed,
                                                    Resigned,
                                                    Date_of_Birth,
                                                    Country_of_Residence,
                                                    Nationality])
            else:
                with open(os.path.abspath(
                          os.path.join(ch_data_path,
                                       'ch_full_officers.tsv')),
                          'a', encoding='latin-1') as tsvfile:
                    officers_data = csv.writer(
                        tsvfile, delimiter='\t',
                        lineterminator='\n')
                    officers_data.writerow([CompanyNumber, 'N\A', 'N\A', 'N\A',
                                            'N\A', 'N\A', 'N\A'])
        else:
            with open(os.path.abspath(
                      os.path.join(ch_data_path,
                                   'ch_full_officers.tsv')),
                      'a', encoding='latin-1') as tsvfile:
                officers_data = csv.writer(
                    tsvfile, delimiter='\t',
                    lineterminator='\n')
                officers_data.writerow([CompanyNumber, 'N\A', 'N\A', 'N\A',
                                        'N\A', 'N\A', 'N\A'])


if __name__ == '__main__':
    ch_data_path = os.path.abspath(
        os.path.join(__file__, '../..', 'data', 'companies_house'))
    make_psc_flatfile()
    APIKey = load_token()
    call_people.counter = 0
    scrape_full_officer_database(APIKey)
