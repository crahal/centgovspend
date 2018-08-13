import os
import traceback
from tqdm import tqdm
from unidecode import unidecode
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from ratelimit import limits, sleep_and_retry
from time import gmtime, strftime
import json
import logging
tqdm.monitor_interval = 0


@sleep_and_retry
@limits(calls=550, period=600)
def call_ch_api(urltocall, logfilehandler=None, pars=None):
    ''' call ch apis in a way that can be rate limited'''
    if pars is None:
        apireturn = requests.get(urltocall, auth=HTTPBasicAuth(APIKey, ''))
    else:
        apireturn = requests.get(urltocall, auth=HTTPBasicAuth(APIKey, ''),
                                 params=pars)
    if logfilehandler is not None:
        with open(logfilehandler, 'a') as f:
            if apireturn.status_code != 200:
                f.write(str(urltocall +
                            ': ' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) +
                            '\n'))
            else:
                f.write('ERROR! ' + str(format(apireturn.status_code)) + ': ' +
                        str(urltocall +
                            ': ' + strftime("%Y-%m-%d %H:%M:%S", gmtime()) +
                            '\n''\n'))
    return apireturn


def load_token():
    try:
        with open('ch_apikey', 'r') as file:
            return str(file.readline()).strip()
    except EnvironmentError:
        print('Error loading access token from file')


module_logger = logging.getLogger('centgovspend_application')
baseOC = 'https://opencorporates.com/reconcile/gb?'
APIKey = load_token()
CH = 'https://api.companieshouse.gov.uk/company/'


def best_reconcile(js, typereconcile='topscore'):
    if typereconcile == 'topscore':
        return js['result'][0]['id'], js['result'][0]['name']


def ch_basic(id, i):
    r = call_ch_api(CH + str(id), None, None)
    if r.status_code == 200:
        js = json.loads(r.content)
        module_logger.info('Successfully got CH basic data for ' + i)
        return js
    else:
        module_logger.info('Wuh oh! Failed getting Basic data for ' + i)
        return None


def ch_officers(id, i):
    ''' master function for calling the officers api, appending the jsons
    into a list across various pages
    '''

    jsonlist = []
    pages = 1
    pars = {'items_per_page': '100',
            'start_index': str(((pages - 1) * 100))}
    officers = call_ch_api(CH + str(id) + '/officers', None, pars)
    if officers.status_code == 200:
        module_logger.info('Successfully got officer data for ' + i)
        jsonlist.append(officers.json())
        try:
            numberofficers = officers.json()['total_results']
            while pages < (numberofficers / 100):
                pages += 1
                pars = {'items_per_page': '100',
                        'start_index': str(((pages - 1) * 100))}
                officers = call_ch_api(CH + str(id) + '/officers', None, pars)
                jsonlist.append(officers.json())
        except KeyError:
            print('Whats going on with ' + str(id))
    return jsonlist


def ch_psc(id, i):
    ''' master function for calling the psc api, appending the jsons
    into a list across various pages
    '''

    jsonlist = []
    pages = 1
    pars = {'items_per_page': '100',
            'start_index': str(((pages - 1) * 100))}
    psc_people = call_ch_api(CH + str(id) +
                             '/persons-with-significant-control', None, pars)
    if psc_people.status_code == 200:
        jsonlist.append(psc_people.json())
        module_logger.info('Successfully got psc data for ' + i)
        try:
            numberofficers = psc_people.json()['total_results']
            while pages < (numberofficers / 100):
                pages += 1
                pars = {'items_per_page': '100',
                        'start_index': str(((pages - 1) * 100))}
                psc_people = call_ch_api(CH + str(id) +
                                         '/persons-with-significant-control',
                                         None, pars)
                jsonlist.append(psc_people.json())
        except KeyError:
            print('Whats going on with ' + str(id))
    return jsonlist


@sleep_and_retry
@limits(calls=33, period=100)
def get_opencorporates(queryterm, limit):
    queryterm = unidecode(queryterm)
    queryterm = queryterm.replace('&', 'and')  # api doesnt like
    queryterm = queryterm.replace('#', '')  # api doesnt like
    queryterm = queryterm.replace('/', '')  # api doesnt like
    queryterm = queryterm.replace('\\', '')  # api doesnt like
    queryterm = queryterm.replace('(', '')  # api doesnt like
    queryterm = queryterm.replace(')', '')  # api doesnt like
    url = baseOC + 'query={"query":"' + \
        queryterm + '", "limit": ' + str(limit) + '}'
    try:
        r = requests.get(url)
        if r.status_code == 200:
            js = json.loads(r.content)
            try:
                return js
            except Exception as e:
                return None
        else:
            module_logger.debug('Non-200 response for ' + queryterm)
            return None
    except Exception as e:
        module_logger.debug(queryterm + ':' + str(e))


def reconcile_dataframe(rawpath, uniquesups):
    print('\n>> Now working on reconciling the unique suppliers!\n')
    if os.path.exists(os.path.abspath(
        os.path.join(__file__, '../..', 'data',
                     'output', 'master',
                     'Reconciled_Suppliers.tsv'))):
        df = pd.read_csv(os.path.abspath(
            os.path.join(__file__, '../..', 'data',
                         'output', 'master',
                         'Reconciled_Suppliers.tsv')),
                         encoding="ISO-8859-1", sep='\t')
        pbar = tqdm(list(set(uniquesups['supplier'].tolist()) -
                         set(df['RawSupplier'].tolist())))
    else:
        with open(os.path.abspath(
            os.path.join(__file__, '../..', 'data',
                         'output', 'master',
                         'Reconciled_Suppliers.tsv')), 'w') as basic_suppliers:
            basic_suppliers.write('RawSupplier\tFirst ID\tFirst Match\t'
                                  'First Score\tSecond ID\tSecond Match\t'
                                  'Second Score\tThird ID\tThird Match\t'
                                  'Third Score\tCompany Status\t'
                                  'Date of Creation\tJurisdiction\t'
                                  'Address Line 1\tAddress Line 2\tLocality\t'
                                  'Postcode\tDisputed Office\t'
                                  'SIC Code\tType\tBest ID\tBest Match\n')
        pbar = tqdm(uniquesups['supplier'].tolist())
    if os.path.exists(os.path.abspath(
        os.path.join(__file__, '../..', 'data',
                     'output', 'master',
                     'Reconciled_Officers.tsv'))) is False:
        with open(os.path.abspath(
            os.path.join(__file__, '../..', 'data',
                         'output', 'master',
                         'Reconciled_Officers.tsv')), 'w') as officers_data:
            officers_data.write('Best ID\tBest Match\tName\tOfficer ID\t'
                                'Appointed\tResigned\tOccupation\t'
                                'Officer Role\tDate of Birth\t'
                                'Country of Residence\tNationality\t'
                                'address_line_1\taddress_line_2\t'
                                'locality\tpostal_code\tregion\n')
    if os.path.exists(os.path.abspath(
        os.path.join(__file__, '../..', 'data',
                     'output', 'master',
                     'Reconciled_PSC.tsv'))) is False:
        with open(os.path.abspath(
            os.path.join(__file__, '../..', 'data',
                         'output', 'master',
                         'Reconciled_PSC.tsv')), 'w') as officers_data:
            officers_data.write('Best ID\tBest Match\t'
                                'Name\tetag\tcountry_registered\t'
                                'legal_authority\tlegal_form\t'
                                'place_registered\tregistration_number\t'
                                'kind\tnatures_of_control\t'
                                'notified_on\taddress_line_1\taddress_line_2\t'
                                'locality\tpostal_code\tregion\t'
                                'premises\tcountry_of_residence\tnationality\t'
                                'forename\tmiddle_name\tsurname\ttitle\n')
    for i in pbar:
        i = unidecode(i)
        pbar.set_description("Processing %s" % i)
        RawSupplier = i.replace('\t', '').replace('\n', '').replace('\r', '')
        js = get_opencorporates(unidecode(i), 3)
        try:
            First_ID = js['result'][0]['id']
            module_logger.info('Successfully reconciled ' + i)
        except Exception as e:
            First_ID = 'N/A'
            module_logger.info('No reconciliations for ' + i)
        try:
            First_Match = js['result'][0]['name']
        except Exception as e:
            First_Match = 'N/A'
        try:
            First_Score = str(js['result'][0]['score'])
        except Exception as e:
            First_Score = 'N/A'
        try:
            Second_ID = js['result'][1]['id']
        except Exception as e:
            Second_ID = 'N/A'
        try:
            Second_Match = js['result'][1]['name']
        except Exception as e:
            Second_Match = 'N/A'
        try:
            Second_Score = str(js['result'][1]['score'])
        except Exception as e:
            Second_Score = 'N/A'
        try:
            Third_ID = js['result'][2]['id']
        except Exception as e:
            Third_ID = 'N/A'
        try:
            Third_Match = js['result'][2]['name']
        except Exception as e:
            Third_Match = 'N/A'
        try:
            Third_Score = str(js['result'][2]['score'])
        except Exception as e:
            Third_Score = 'N/A'
        if First_ID != 'N/A':
            bestid, bestmatch = best_reconcile(js)
            chjs = ch_basic(bestid.replace('/companies/gb/', ''), i)
        else:
            bestid = 'N/A'
            bestmatch = 'N/A'
            chjs = None
        try:
            Company_Status = chjs['company_status']
        except Exception as e:
            Company_Status = 'N/A'
        try:
            Date_of_Creation = chjs['date_of_creation']
        except Exception as e:
            Date_of_Creation = 'N/A'
        try:
            Jurisdiction = chjs['jurisdiction']
        except Exception as e:
            Jurisdiction = 'N/A'
        try:
            Address_Line_1 = chjs['registered_office_address']['address_line_1']
        except Exception as e:
            Address_Line_1 = 'N/A'
        try:
            Address_Line_2 = chjs['registered_office_address']['address_line_2']
        except Exception as e:
            Address_Line_2 = 'N/A'
        try:
            Locality = chjs['registered_office_address']['locality']
        except Exception as e:
            Locality = 'N/A'
        try:
            Postcode = chjs['registered_office_address']['postal_code']
        except Exception as e:
            Postcode = 'N/A'
        try:
            Disputed_Office = chjs['registered_office_is_in_dispute']
        except Exception as e:
            Disputed_Office = 'N/A'
        try:
            SIC_Code = chjs['sic_codes']
        except Exception as e:
            SIC_Code = 'N/A'
        try:
            Type = chjs['type']
        except Exception as e:
            Type = 'N/A'
        with open(os.path.abspath(
            os.path.join(__file__, '../..', 'data',
                         'output', 'master', 'Reconciled_Suppliers.tsv')),
                  'a') as basic_suppliers:
            basic_suppliers.write(RawSupplier + '\t' +
                                  First_ID + '\t' +
                                  First_Match + '\t' +
                                  str(First_Score) + '\t' +
                                  Second_ID + '\t' +
                                  Second_Match + '\t' +
                                  str(Second_Score) + '\t' +
                                  Third_ID + '\t' +
                                  Third_Match + '\t' +
                                  str(Third_Score) + '\t' +
                                  Company_Status + '\t' +
                                  Date_of_Creation + '\t' +
                                  Jurisdiction + '\t' +
                                  Address_Line_1 + '\t' +
                                  Address_Line_2 + '\t' +
                                  Locality + '\t' +
                                  Postcode + '\t' +
                                  str(Disputed_Office) + '\t' +
                                  str(SIC_Code) + '\t' +
                                  Type + '\t' +
                                  bestid + '\t' +
                                  bestmatch + '\n')
        if bestid != 'N/A':
            try:
                offjs = ch_officers(bestid.replace('/companies/gb/', ''), i)
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
                                os.path.join(__file__, '../..', 'data',
                                             'output', 'master',
                                             'Reconciled_Officers.tsv')),
                                      'a') as officers_data:
                                officers_data.write(bestid + '\t' +
                                                    bestmatch + '\t' +
                                                    Name + '\t' +
                                                    Officer_ID + '\t' +
                                                    Appointed + '\t' +
                                                    Resigned + '\t' +
                                                    Occupation + '\t' +
                                                    Officer_Role + '\t' +
                                                    Date_of_Birth + '\t' +
                                                    Country_of_Residence + '\t' +
                                                    Nationality + '\t' +
                                                    address_line_1 + '\t' +
                                                    address_line_2 + '\t' +
                                                    locality + '\t' +
                                                    postal_code + '\t' +
                                                    region + '\n')
            except Exception as e:
                module_logger.debug(
                    'Something wrong with officers API: ' + i + ': ' + str(e))
                traceback.print_tb(e.__traceback__)
        if bestid != 'N/A':
            try:
                pscjs = ch_psc(bestid.replace('/companies/gb/', ''), i)
                if pscjs is not None:
                    for page in pscjs:
                        for psc in page['items']:
                            try:
                                Name = psc['name']
                            except KeyError:
                                Name = 'N/A'
                            try:
                                etag = psc['etag']
                            except KeyError:
                                etag = 'N/A'
                            try:
                                country_registered = psc['identification']['country_registered']
                            except KeyError:
                                country_registered = 'N/A'
                            try:
                                legal_authority = psc['identification']['legal_authority']
                            except KeyError:
                                legal_authority = 'N/A'
                            try:
                                legal_form = psc['identification']['legal_form']
                            except KeyError:
                                legal_form = 'N/A'
                            try:
                                place_registered = psc['identification']['place_registered']
                            except KeyError:
                                place_registered = 'N/A'
                            try:
                                registration_number = psc['identification']['registration_number']
                            except KeyError:
                                registration_number = 'N/A'
                            try:
                                kind = psc['kind']
                            except KeyError:
                                kind = 'N/A'
                            try:
                                natures_of_control = str(
                                    psc['natures_of_control'])
                            except KeyError:
                                natures_of_control = 'N/A'
                            try:
                                notified_on = psc['notified_on']
                            except KeyError:
                                notified_on = 'N/A'
                            try:
                                address_line_1 = psc['address']['address_line_1']
                            except KeyError:
                                address_line_1 = 'N/A'
                            try:
                                address_line_2 = psc['address']['address_line_2']
                            except KeyError:
                                address_line_2 = 'N/A'
                            try:
                                locality = psc['address']['locality']
                            except KeyError:
                                locality = 'N/A'
                            try:
                                postal_code = psc['address']['postal_code']
                            except KeyError:
                                postal_code = 'N/A'
                            try:
                                region = psc['address']['region']
                            except KeyError:
                                region = 'N/A'
                            try:
                                premises = psc['address']['premises']
                            except KeyError:
                                premises = 'N/A'
                            try:
                                country_of_residence = psc['country_of_residence']
                            except KeyError:
                                country_of_residence = 'N/A'
                            try:
                                nationality = psc['nationality']
                            except KeyError:
                                nationality = 'N/A'
                            try:
                                forename = psc['name_elements']['forename']
                            except KeyError:
                                forename = 'N/A'
                            try:
                                middle_name = psc['name_elements']['middle_name']
                            except KeyError:
                                middle_name = 'N/A'
                            try:
                                surname = psc['name_elements']['surname']
                            except KeyError:
                                surname = 'N/A'
                            try:
                                title = psc['name_elements']['title ']
                            except KeyError:
                                title = 'N/A'
                            with open(os.path.abspath(
                                os.path.join(__file__, '../..', 'data',
                                             'output', 'master',
                                             'Reconciled_PSC.tsv')),
                                      'a') as psc_data:
                                psc_data.write(bestid + '\t' +
                                               bestmatch + '\t' +
                                               Name + '\t' +
                                               etag + '\t' +
                                               country_registered + '\t' +
                                               legal_authority + '\t' +
                                               legal_form + '\t' +
                                               place_registered + '\t' +
                                               registration_number + '\t' +
                                               kind + '\t' +
                                               natures_of_control + '\t' +
                                               notified_on + '\t' +
                                               address_line_1 + '\t' +
                                               address_line_2 + '\t' +
                                               locality + '\t' +
                                               postal_code + '\t' +
                                               region + '\t' +
                                               premises + '\t' +
                                               country_of_residence + '\t' +
                                               nationality + '\t' +
                                               forename + '\t' +
                                               middle_name + '\t' +
                                               surname + '\t' +
                                               title + '\n')
            except Exception as e:
                module_logger.debug(
                    'Something wrong with PSC API: ' + i + ': ' + str(e))
                traceback.print_tb(e.__traceback__)
