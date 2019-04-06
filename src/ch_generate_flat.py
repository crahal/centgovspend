import csv
import glob
import datetime
import numpy as np
import time
import json
from ratelimit import limits, sleep_and_retry
from ch_supportfunctions import make_blau
import requests
import os
import zipfile
import gzip
import io
from tqdm import tqdm
import pandas as pd
from ch_load_data import sicmaker
from requests.auth import HTTPBasicAuth


def generate_officer_flat(APIKey, off_flatfile, ch_basic):
    call_off.counter = 0
    bulk_ch = pd.read_csv(ch_basic, sep=',',
                          usecols=[' CompanyNumber'])
    if os.path.exists(off_flatfile) is False:
        with open(off_flatfile, 'w') as tsvfile:
            off_data = csv.writer(tsvfile, delimiter='\t',
                                  lineterminator='\n')
            off_data.writerow(['CompanyNumber', 'Name', 'Officer ID',
                               'Appointed', 'Resigned', 'Occupation',
                               'Officer Role', 'Date of Birth',
                               'Country of Residence', 'Nationality',
                               'address_line_1', 'address_line_2',
                               'locality', 'postal_code', 'region'])
        listofcompanies = bulk_ch[' CompanyNumber'].tolist()
    else:
        existing_flatfile = pd.read_csv(off_flatfile,
                                        encoding='latin-1', sep='\t',
                                        error_bad_lines=False,
                                        warn_bad_lines=False)
        listofcompanies = list(set(bulk_ch[' CompanyNumber']
                                   .tolist()) -
                               set(existing_flatfile['CompanyNumber']
                                     .tolist()))
    pbar = tqdm(listofcompanies)
    pbar.set_description("Building Officer Dataset")
    for CompanyNumber in pbar:
        offjs = ch_off(CompanyNumber, APIKey)
        if len(offjs) > 0:
            if offjs[0]['active_count'] > 0:
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
                        with open(off_flatfile,
                                  'a', encoding='latin-1',
                                  errors='ignore') as tsvfile:
                            off_data = csv.writer(tsvfile, delimiter='\t',
                                                  lineterminator='\n')
                            off_data.writerow([CompanyNumber, Name,
                                               Officer_ID, Appointed,
                                               Resigned, Occupation,
                                               Officer_Role,
                                               Date_of_Birth,
                                               Country_of_Residence,
                                               Nationality,
                                               address_line_1,
                                               address_line_2, locality,
                                               postal_code, region])
            else:
                with open(off_flatfile,
                          'a', encoding='latin-1') as tsvfile:
                    off_data = csv.writer(tsvfile, delimiter='\t',
                                          lineterminator='\n')
                    off_data.writerow([CompanyNumber, 'N\A', 'N\A', 'N\A',
                                       'N\A', 'N\A', 'N\A', 'N\A', 'N\A',
                                       'N\A', 'N\A', 'N\A', 'N\A', 'N\A',
                                       'N\A'])
        else:
            with open(off_flatfile,
                      'a', encoding='latin-1') as tsvfile:
                off_data = csv.writer(
                    tsvfile, delimiter='\t',
                    lineterminator='\n')
                off_data.writerow([CompanyNumber, 'N\A', 'N\A', 'N\A',
                                   'N\A', 'N\A', 'N\A', 'N\A', 'N\A',
                                   'N\A', 'N\A', 'N\A', 'N\A', 'N\A',
                                   'N\A'])


def generate_psc_flat(psc_rawdata, psc_flatfile):
    with open(psc_flatfile, 'w') as tsvfile:
        psc_data = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')
        psc_data.writerow(['company_number', 'address_line_1',
                           'address_line_2', 'locality', 'postal_code',
                           'region', 'country_of_residence', 'date_of_birth',
                           'etag', 'kind', 'links', 'name', 'forename',
                           'middle_name', 'surname', 'title', 'nationality',
                           'natures_of_control'])
        with open(psc_rawdata, 'r', encoding='latin-1') as f:
            pbar = tqdm(f)
            pbar.set_description("Processing PSC Flatfile")
            for line in pbar:
                jsonfile = json.loads(line)
                try:
                    company_number = jsonfile['company_number']
                except Exception as e:
                    company_number = 'N/A'
                try:
                    address_line_1 = jsonfile['data']['address']['address_line_1']
                except Exception as e:
                    address_line_1 = 'N/A'
                try:
                    address_line_2 = jsonfile['data']['address']['address_line_2']
                except Exception as e:
                    address_line_2 = 'N/A'
                try:
                    locality = jsonfile['data']['address']['locality']
                except Exception as e:
                    locality = 'N/A'
                try:
                    postal_code = jsonfile['data']['address']['postal_code']
                except Exception as e:
                    postal_code = 'N/A'
                try:
                    region = jsonfile['data']['address']['region']
                except Exception as e:
                    region = 'N/A'
                try:
                    country_of_residence = jsonfile['data']['country_of_residence']
                except Exception as e:
                    country_of_residence = 'N/A'
                try:
                    date_of_birth = jsonfile['data']['date_of_birth']
                except Exception as e:
                    date_of_birth = 'N/A'
                try:
                    etag = jsonfile['data']['etag']
                except Exception as e:
                    etag = 'N/A'
                try:
                    kind = jsonfile['data']['kind']
                except Exception as e:
                    kind = 'N/A'
                try:
                    links = jsonfile['data']['links']
                except Exception as e:
                    links = 'N/A'
                try:
                    name = jsonfile['data']['name']
                except Exception as e:
                    name = 'N/A'
                try:
                    forename = jsonfile['data']['name_elements']['forename']
                except Exception as e:
                    forename = 'N/A'
                try:
                    middle_name = jsonfile['data']['name_elements']['middle_name']
                except Exception as e:
                    middle_name = 'N/A'
                try:
                    surname = jsonfile['data']['name_elements']['surname']
                except Exception as e:
                    surname = 'N/A'
                try:
                    title = jsonfile['data']['name_elements']['title']
                except Exception as e:
                    title = 'N/A'
                try:
                    nationality = jsonfile['data']['nationality']
                except Exception as e:
                    nationality = 'N/A'
                try:
                    natures_of_control = jsonfile['data']['natures_of_control']
                except Exception as e:
                    natures_of_control = 'N/A'
                try:
                    psc_data.writerow([company_number,
                                       address_line_1,
                                       address_line_2,
                                       locality,
                                       postal_code,
                                       region,
                                       country_of_residence,
                                       str(date_of_birth),
                                       etag, kind,
                                       str(links),
                                       name,
                                       forename,
                                       middle_name,
                                       surname,
                                       title,
                                       nationality,
                                       str(natures_of_control)])
                except:
                    pass


@sleep_and_retry
@limits(calls=600, period=600)
def call_off(urltocall, logfilehandler, APIKey, pars=None):
    ''' call people apis in a way that can be rate limited'''

    call_off.counter += 1
    people = requests.get(urltocall, auth=HTTPBasicAuth(APIKey, ''),
                          params=pars)
    with open(logfilehandler, 'a') as f:
        f.write(str(format(people.status_code)) + ': ' +
                str(call_off.counter) + ': ' + urltocall +
                ': ' + datetime.strftime("%Y-%m-%d %H:%M:%S",
                                           time.gmtime()) + '\n')
    return people


def ch_off(id, APIKey):
    ''' master function for calling the off api, appending the jsons
    into a list across various pages
    '''

    jsonlist = []
    CH = 'https://api.companieshouse.gov.uk/company/'
    pages = 1
    pars = {'items_per_page': '100',
            'start_index': str(((pages - 1) * 100))}
    url_id = CH + str(id) + '/officers'
    try:
        off = call_off(url_id, 'full_ch_off.log', APIKey, pars)
        if off.status_code == 200:
            jsonlist.append(off.json())
            try:
                numberoff = off.json()['total_results']
                while pages < (numberoff / 100):
                    pages += 1
                    pars = {'items_per_page': '100',
                            'start_index': str(((pages - 1) * 100))}
                    off = call_off(url_id, 'full_ch_off.log',
                                           APIKey, pars)
                    jsonlist.append(off.json())
            except KeyError:
                print('Whats going on with ' + str(id))
    except:
        time.sleep(60)
    return jsonlist


def originate_psc(ch_url, psc_filename, CH_Data):
    if os.path.isfile(os.path.join(CH_Data, 'psc_flatfile.tsv.gz')) is False:
        psc_filename = 'persons-with-significant-control-snapshot-2019-01-24'
        r = requests.get(ch_url + psc_filename + '.zip')
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(os.path.join(CH_Data))
        generate_psc_flat(os.path.join(CH_Data, psc_filename + '.txt'),
                          os.path.join(CH_Data, 'psc_flatfile.tsv'))
        f_in = open(os.path.join(CH_Data, 'psc_flatfile.tsv'), 'rb')
        f_out = gzip.open(os.path.join(CH_Data, 'psc_flatfile.tsv.gz'), 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
        os.remove(os.path.join(os.path.join(CH_Data, psc_filename + '.txt')))
        os.remove(os.path.join(os.path.join(CH_Data, 'psc_flatfile.tsv')))


def originate_basic(ch_url, basic_filename, CH_Data):
    if os.path.isfile(os.path.join(CH_Data,
                                   basic_filename + '.csv.gz')) is False:
        r = requests.get(ch_url + basic_filename + '.zip')
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(os.path.join(CH_Data))
        f_in = open(os.path.join(CH_Data, basic_filename + '.csv'), 'rb')
        f_out = gzip.open(os.path.join(
            CH_Data, basic_filename + '.csv.gz'), 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
        os.remove(os.path.join(os.path.join(CH_Data,
                                            basic_filename + '.csv')))


def generate_accounts(CH_Data, Local_Data, ch_basic):
    allFiles = glob.glob(os.path.join(CH_Data, 'accounts_data') + "/*.csv")
    list_ = []
    for file_ in allFiles:
        df = pd.read_csv(file_,
                         index_col=None,
                         header=0,
                         engine='python',
                         error_bad_lines=False,
                         warn_bad_lines=False)
        list_.append(df)
    frame = pd.concat(list_, axis=0, ignore_index=True)
    frame['name'] = frame['name'].str.replace('[^a-zA-Z]', '')
    frame.groupby('name').size().reset_index().rename({0:
                                                       'count'},
                                                      axis=1).sort_values(by='count',
                                                                          ascending=False).set_index('name')
    Cur_Ass = frame[frame['name'] == 'CurrentAssets']
    Cur_Ass['ch_num'] = Cur_Ass['file_name'].str.split('_', expand=True)[2]
    Cur_Ass = Cur_Ass.set_index('ch_num').rename(
        {'value': 'CurrentAssets'}, axis=1)
    Cur_Ass = Cur_Ass.drop(['name', 'unit', 'file_name'], axis=1)

    Net_Ass = frame[frame['name'] == 'NetCurrentAssetsLiabilities']
    Net_Ass['ch_num'] = Net_Ass['file_name'].str.split('_', expand=True)[2]
    Net_Ass = Net_Ass.set_index('ch_num').rename({'value':
                                                  'NetCurrentAssetsLiabilities'},
                                                 axis=1)
    Net_Ass = Net_Ass.drop(['name', 'unit', 'file_name'], axis=1)

    ch_accounts = pd.merge(Cur_Ass, Net_Ass, how='left',
                           left_index=True, right_index=True)

    Tot_Ass = frame[frame['name'] == 'TotalAssetsLessCurrentLiabilities']
    Tot_Ass['ch_num'] = Tot_Ass['file_name'].str.split('_', expand=True)[2]
    Tot_Ass = Tot_Ass.set_index('ch_num').rename({'value':
                                                  'TotalAssetsLessCurrentLiabilities'}, axis=1)
    Tot_Ass = Tot_Ass.drop(['name', 'unit', 'file_name'], axis=1)
    ch_accounts = pd.merge(ch_accounts, Tot_Ass, how='left',
                           left_index=True, right_index=True)

    Pro_Los = frame[frame['name'] == 'ProfitLoss']
    Pro_Los['ch_num'] = Pro_Los['file_name'].str.split('_', expand=True)[2]
    Pro_Los = Pro_Los.set_index('ch_num').rename({'value':
                                                  'ProfitLoss'}, axis=1)
    Pro_Los = Pro_Los.drop(['name', 'unit', 'file_name'], axis=1)
    ch_accounts = pd.merge(ch_accounts, Pro_Los, how='left',
                           left_index=True, right_index=True)
    Pro_Pla = frame[frame['name'] == 'PropertyPlantEquipmentGrossCost']
    Pro_Pla['ch_num'] = Pro_Pla['file_name'].str.split('_', expand=True)[2]
    Pro_Pla = Pro_Pla.set_index('ch_num').rename({'value':
                                                  'PropertyPlantEquipmentGrossCost'}, axis=1)
    Pro_Pla = Pro_Pla.drop(['name', 'unit', 'file_name'], axis=1)
    ch_accounts = pd.merge(ch_accounts, Pro_Pla, how='left',
                           left_index=True, right_index=True)

    Fix_Ass = frame[frame['name'] == 'FixedAssets']
    Fix_Ass['ch_num'] = Fix_Ass['file_name'].str.split('_', expand=True)[2]
    Fix_Ass = Fix_Ass.set_index('ch_num').rename({'value':
                                                  'FixedAssets'}, axis=1)
    Fix_Ass = Fix_Ass.drop(['name', 'unit', 'file_name'], axis=1)
    ch_accounts = pd.merge(ch_accounts, Fix_Ass, how='left',
                           left_index=True, right_index=True)

    Num_Emp = frame[frame['name'] == 'AverageNumberEmployeesDuringPeriod']
    Num_Emp['ch_num'] = Num_Emp['file_name'].str.split('_', expand=True)[2]
    Num_Emp = Num_Emp.set_index('ch_num').rename({'value':
                                                  'AverageNumberEmployeesDuringPeriod'}, axis=1)
    Num_Emp = Num_Emp.drop(['name', 'unit', 'file_name'], axis=1)
    ch_accounts = pd.merge(ch_accounts, Num_Emp, how='left',
                           left_index=True, right_index=True)

    ch_accounts = ch_accounts.reset_index().\
        drop_duplicates(subset=['NetCurrentAssetsLiabilities',
                                'TotalAssetsLessCurrentLiabilities',
                                'ch_num'], keep='first')
    ch_accounts = ch_accounts.drop_duplicates(subset=['ch_num'], keep=False)
    ch_accounts = ch_accounts.set_index('ch_num')

    ch_accounts = ch_accounts[ch_accounts.index.isin(
        list(ch_basic[' CompanyNumber']))]
    ch_accounts.to_csv(os.path.join(Local_Data, 'engineered',
                                    'ch_accounts_2017.csv.gz'),
                       sep='\t',
                       compression='gzip')


def ch_generate_regdf(ch_off, ch_basic, ch_psc, ch_accounts, off_count=1):
    ''' build the dataframe for the diveristy regressions '''
    ch_off = ch_off[ch_off['Officer Role'] == 'director']
    ch_off = ch_off[ch_off['gender'].notnull()]
    ch_off = ch_off[ch_off['Age'].notnull()]
    ch_off = ch_off[ch_off['nationality_cleaned'].notnull()]
    ch_off = ch_off[ch_off['Resigned'].isnull()]
    ch_reg = pd.merge(pd.DataFrame(ch_off[ch_off['Resigned'].isnull()].
                                   groupby('CompanyNumber')['CompanyNumber'].
                                   count()).
                      rename({'CompanyNumber': 'OfficerCount'},
                             axis=1).reset_index(),
                      pd.DataFrame(ch_off[ch_off['Resigned'].isnull()].
                                   groupby('CompanyNumber')['Age'].mean()).
                      rename({'Age': 'AgeMean'}, axis=1).
                      reset_index(),
                      how='left',
                      left_on='CompanyNumber', right_on='CompanyNumber')
    ch_reg = pd.merge(ch_reg,
                      pd.DataFrame(ch_psc.
                                   groupby('company_number')['company_number'].
                                   count()).
                      rename({'company_number': 'PSCCount'},
                             axis=1).reset_index(),
                      how='left',
                      left_on='CompanyNumber', right_on='company_number')
    ch_basic['SIC_INT'] = ch_basic['SICCode.SicText_1'].str.extract('(\d+)')
    ch_basic['SIC_INT'] = pd.to_numeric(ch_basic['SIC_INT'])
    ch_basic['SIMPLE_SIC'] = ch_basic['SIC_INT'].map(lambda x: sicmaker(x))
    ch_reg = pd.merge(ch_reg, ch_basic,
                      how='left', left_on='CompanyNumber',
                      right_on=' CompanyNumber')
    ch_reg['CompanyAge'] = 2019 - pd.to_numeric(ch_reg['IncorporationDate'].
                                                str.split('/', expand=True)[2])
    ch_reg = pd.merge(ch_reg, ch_accounts,
                      how='left',
                      left_on='CompanyNumber',
                      right_on='ch_num')
    ch_reg = ch_reg.drop(columns=['SICCode.SicText_1',
                                  'IncorporationDate',
                                  'SIC_INT', 'ch_num',
                                  ' CompanyNumber',
                                  'company_number'])
    ch_reg = ch_reg[((ch_reg['CurrentAssets'].notnull()) |
                    (ch_reg['NetCurrentAssetsLiabilities'].notnull()) |
                    (ch_reg['TotalAssetsLessCurrentLiabilities'].notnull()) |
                     (ch_reg['ProfitLoss'].notnull())) &
                    (ch_reg['AverageNumberEmployeesDuringPeriod'].notnull())]
    ch_reg = ch_reg[ch_reg['OfficerCount'] > off_count]
    ch_off['Age_bin'] = np.nan
    ch_off['Age_bin'] = np.where(ch_off.Age.astype(float) < 10, 'Under 10',
                                 ch_off.Age_bin)
    ch_off['Age_bin'] = np.where((ch_off.Age.astype(float) >= 10) &
                                 (ch_off.Age.astype(float) < 20), '10-20',
                                 ch_off.Age_bin)
    ch_off['Age_bin'] = np.where((ch_off.Age.astype(float) >= 20) &
                                 (ch_off.Age.astype(float) < 30), '20-30',
                                 ch_off.Age_bin)
    ch_off['Age_bin'] = np.where((ch_off.Age.astype(float) >= 30) &
                                 (ch_off.Age.astype(float) < 40), '30-40',
                                 ch_off.Age_bin)
    ch_off['Age_bin'] = np.where((ch_off.Age.astype(float) >= 40) &
                                 (ch_off.Age.astype(float) < 50), '40-50',
                                 ch_off.Age_bin)
    ch_off['Age_bin'] = np.where((ch_off.Age.astype(float) >= 50) &
                                 (ch_off.Age.astype(float) < 60), '50-60',
                                 ch_off.Age_bin)
    ch_off['Age_bin'] = np.where((ch_off.Age.astype(float) >= 60) &
                                 (ch_off.Age.astype(float) < 70), '60-70',
                                 ch_off.Age_bin)
    ch_off['Age_bin'] = np.where((ch_off.Age.astype(float) >= 70) &
                                 (ch_off.Age.astype(float) < 80), '70-80',
                                 ch_off.Age_bin)
    ch_off['Age_bin'] = np.where((ch_off.Age.astype(float) >= 80) &
                                 (ch_off.Age.astype(float) < 90), '80-90',
                                 ch_off.Age_bin)
    ch_off['Age_bin'] = np.where(ch_off.Age.astype(float) > 90, 'Over 90',
                                 ch_off.Age_bin)
    ch_off = ch_off[['CompanyNumber', 'nationality_cleaned',
                     'gender', 'Age_bin']]
    ch_off = ch_off[ch_off['CompanyNumber'].isin(ch_reg['CompanyNumber'])]
    ch_reg['ROI'] = (ch_reg['ProfitLoss']
                     / ch_reg['TotalAssetsLessCurrentLiabilities']) * 100
    ch_reg = ch_reg.set_index('CompanyNumber')
    for company in tqdm(ch_reg.index):
        temp_off = ch_off[ch_off['CompanyNumber'] == company]
        ch_reg.at[ch_reg.index == company,
                  'blau_gender'] = make_blau(temp_off['gender'])
        ch_reg.at[ch_reg.index == company,
                  'blau_nationality'] = make_blau(temp_off['nationality_cleaned'])
        ch_reg.at[ch_reg.index == company,
                  'blau_age'] = make_blau(temp_off['Age_bin'])

    return ch_reg
