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


@sleep_and_retry
@limits(calls=600, period=600)
def call_people(urltocall, logfilehandler, APIKey, pars=None):
    ''' call people apis in a way that can be rate limited'''

    call_people.counter += 1
    people = requests.get(urltocall, auth=HTTPBasicAuth(APIKey, ''),
                          params=pars)
    with open(logfilehandler, 'a') as f:
        if people.status_code == 200:
            f.write(str(call_people.counter) + ': ' + urltocall +
                    ': ' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '\n')
        else:
            f.write('ERROR! ' + str(format(people.status_code)) + ': ' +
                    str(call_people.counter) + ': ' + urltocall +
                    ': ' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + '\n')
    return people


def ch_officers(id, i, APIKey):
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
        os.path.join(ch_data_path, 'full_ch_officers.log')), pars, APIKey)
    if officers.status_code == 200:
        jsonlist.append(officers.json())
        try:
            numberofficers = officers.json()['total_results']
            while pages < (numberofficers / 100):
                pages += 1
                pars = {'items_per_page': '100',
                        'start_index': str(((pages - 1) * 100))}
                officers = call_people(url_id, pars, APIKey)
                jsonlist.append(officers.json())
        except KeyError:
            print('Whats going on with ' + str(id))
    return jsonlist


def make_psc_flatfile():
    with open(os.path.abspath(os.path.join(ch_data_path, 'psc_flatfile.tsv')),
              'w') as tsvfile:
        psc_data = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')
        psc_data.writerows(['company_number','address_line_1','address_line_2',
                            'locality','postal_code','region','country_of_residence',
                            'date_of_birth','etag','kind','links','name','forename',
                            'middle_name','surname','title','nationality',
                            'natures_of_control'])
        with open(os.path.abspath(
                  os.path.join(ch_data_path,
                               'persons-with-significant-control-snapshot-2018-09-07.txt')),
                  'r', encoding='latin1') as f:
            for line in tqdm(f):
                line = json.loads(line)
                try:
                    company_number = line['company_number']
                except Exception as e:
                    company_number = 'N/A'
                try:
                    address_line_1 = json.loads(
                        line)['data']['address']['address_line_1']
                except Exception as e:
                    address_line_1 = 'N/A'
                try:
                    address_line_2 = json.loads(
                        line)['data']['address']['address_line_2']
                except Exception as e:
                    address_line_2 = 'N/A'
                try:
                    locality = line['data']['address']['locality']
                except Exception as e:
                    locality = 'N/A'
                try:
                    postal_code = json.loads(
                        line)['data']['address']['postal_code']
                except Exception as e:
                    postal_code = 'N/A'
                try:
                    region = line['data']['address']['region']
                except Exception as e:
                    region = 'N/A'
                try:
                    country_of_residence = json.loads(
                        line)['data']['country_of_residence']
                except Exception as e:
                    country_of_residence = 'N/A'
                try:
                    date_of_birth = line['data']['date_of_birth']
                except Exception as e:
                    date_of_birth = 'N/A'
                try:
                    etag = line['data']['etag']
                except Exception as e:
                    etag = 'N/A'
                try:
                    kind = line['data']['kind']
                except Exception as e:
                    kind = 'N/A'
                try:
                    links = line['data']['links']
                except Exception as e:
                    links = 'N/A'
                try:
                    name = line['data']['name']
                except Exception as e:
                    name = 'N/A'
                try:
                    forename = json.loads(
                        line)['data']['name_elements']['forename']
                except Exception as e:
                    forename = 'N/A'
                try:
                    middle_name = json.loads(
                        line)['data']['name_elements']['middle_name']
                except Exception as e:
                    middle_name = 'N/A'
                try:
                    surname = line['data']['name_elements']['surname']
                except Exception as e:
                    surname = 'N/A'
                try:
                    title = line['data']['name_elements']['title']
                except Exception as e:
                    title = 'N/A'
                try:
                    nationality = line['data']['nationality']
                except Exception as e:
                    nationality = 'N/A'
                try:
                    natures_of_control = json.loads(
                        line)['data']['natures_of_control']
                except Exception as e:
                    natures_of_control = 'N/A'
                psc_data.writerows([company_number,address_line_1,address_line_2,
                                    locality,postal_code,region,
                                    country_of_residence,str(date_of_birth),etag,
                                    kind,str(links),name,forename,middle_name,
                                    surname,title,nationality,str(natures_of_control)])


def scrape_full_officer_database(APIKey):
    bulk_ch = pd.read_csv(os.path.abspath(
        os.path.join(ch_data_path,
                     'BasicCompanyDataAsOneFile-2018-05-01.csv')), sep=',',
        usecols=[' CompanyNumber'])
    if os.path.exists(os.path.abspath(
        os.path.join(__file__, '../..', 'data',
                     'companies_house',
                     'ch_full_officers.tsv'))) is False:
        with open(os.path.abspath(
            os.path.join(__file__, '../..', 'data',
                         'companies_house',
                         'ch_full_officers.tsv')), 'w') as tsvfile:
            officers_data = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')
            officers_data.writerows(['CompanyNumber','Name','Officer ID',
                                    'Appointed','Resigned','Occupation',
                                    'Officer Role','Date of Birth',
                                    'Country of Residence','Nationality',
                                    'address_line_1','address_line_2',
                                    'locality','postal_code','region'])
        listofcompanies = bulk_ch[' CompanyNumber'].tolist()
    else:
        existing_flatfile = pd.read_csv(os.path.abspath(
            os.path.join(ch_data_path, 'ch_full_officers.tsv')),
            encoding='latin-1', sep='\t')
        listofcompanies = list(set(
            bulk_ch[' CompanyNumber'].tolist()) -
            set(
            existing_flatfile['CompanyNumber'].tolist()))

    for CompanyNumber in listofcompanies:
        offjs = ch_officers(CompanyNumber, CompanyNumber, APIKey)
        if offjs is not None:
            for page in offjs:
                for off in page['items']:
                    try:
                        Name = off['name']
                    except Exception as e:
                        Name = 'N/A'
                    try:
                        Officer_ID = off['links']['officer']['appointments']
                    except Exception as e:
                        Officer_ID = 'N/A'
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
                        Occupation = off['occupation']
                    except Exception as e:
                        Occupation = 'N/A'
                    try:
                        Officer_Role = off['officer_role']
                    except Exception as e:
                        Officer_Role = 'N/A'
                    try:
                        Date_of_Birth = str(off['date_of_birth'])
                    except Exception as e:
                        Date_of_Birth = 'N/A'
                    try:
                        Country_of_Residence = off['country_of_residence']
                    except Exception as e:
                        Country_of_Residence = 'N/A'
                    try:
                        address_line_1 = off['address']['address_line_1']
                    except Exception as e:
                        address_line_1 = 'N/A'
                    try:
                        address_line_2 = off['address']['address_line_2']
                    except Exception as e:
                        address_line_2 = 'N/A'
                    try:
                        locality = off['address']['locality']
                    except Exception as e:
                        locality = 'N/A'
                    try:
                        postal_code = off['address']['postal_code']
                    except Exception as e:
                        postal_code = 'N/A'
                    try:
                        region = off['address']['region']
                    except Exception as e:
                        region = 'N/A'
                    with open(os.path.abspath(
                            os.path.join(ch_data_path, 'ch_full_officers.tsv')),
                            'a') as tsvfile:
                        officers_data = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')
                        officers_data.writerows([CompanyNumber, Name, Officer_ID,
                                                 Appointed, Resigned, Occupation,
                                                 Officer_Role, Date_of_Birth,
                                                 Country_of_Residence, Nationality,
                                                 address_line_1, address_line_2,
                                                 locality, postal_code, region])


if __name__ == '__main__':
    ch_data_path = os.path.abspath(
        os.path.join(__file__, '../..', 'data', 'companies_house'))
    make_psc_flatfile()
    #APIKey = load_token()
    #call_people.counter = 0
    #scrape_full_officer_database(APIKey)
