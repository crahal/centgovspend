import os
import ezodf
import re
import traceback
import requests
import time
import glob
from bs4 import BeautifulSoup
import ntpath
import sys
from unidecode import unidecode
import pandas as pd
import logging
import xlrd
module_logger = logging.getLogger('centgovspend_application')
base = 'https://www.gov.uk/government/'
pubs = base + 'publications/'
data = 'https://data.gov.uk/'


def read_date(date):
    return xlrd.xldate.xldate_as_datetime(date, 0)


def read_ods(filename, sheet_no=0, header=0):
    tab = ezodf.opendoc(filename=filename).sheets[sheet_no]
    df = pd.DataFrame({col[header].value: [x.value for x in col[header + 1:]]
                       for col in tab.columns()})
    df = df.T.reset_index(drop=False).T
    df = df.drop(columns=[list(df)[-1]], axis=1)
    return df.T.reset_index(drop=False).T


def merge_files(rawpath):
    frame = pd.DataFrame()
    list_ = []
    for file_ in glob.glob(os.path.join(rawpath, '..', 'output',
                                        'mergeddepts', '*.csv')):
        df = pd.read_csv(file_, index_col=None, low_memory=False,
                         header=0, encoding='latin-1',
                         dtype={'transactionnumber': str,
                                'amount': float,
                                'supplier': str,
                                'date': str,
                                'expensearea': str,
                                'expensetype': str,
                                'file': str})

        df['dept'] = ntpath.basename(file_)[:-4]
        list_.append(df)
    frame = pd.concat(list_, sort=False)
    frame.dropna(thresh=0.90 * len(df), axis=1, inplace=True)
    if pd.to_numeric(frame['date'], errors='coerce').notnull().all():
        frame['date'] = pd.to_datetime(frame['date'].apply(read_date),
                                       dayfirst=True,
                                       errors='coerce')
    else:
        df['date'] = pd.to_datetime(df['date'],
                                    dayfirst=True,
                                    errors='coerce')
    frame['transactionnumber'] = frame['transactionnumber'].str.replace('[^\w\s]', '')
    frame['transactionnumber'] = frame['transactionnumber'].str.strip("0")
    return frame


def get_data(datalocations, filepath, department, exclusions=[]):
    ''' send data.gov.uk or gov.uk data through here. '''
    for datalocation in datalocations:
        r = requests.get(datalocation)
        listcsvs = []
        listxls = []
        listxlsx = []
        listods = []
        soup = BeautifulSoup(r.content, 'lxml')
        for link in soup.findAll('a'):
            if link.get('href').lower().endswith('.csv'):
                if 'data.gov.uk' in datalocation:
                    listcsvs.append(link.get('href'))
                else:
                    listcsvs.append('https://www.gov.uk/' + link.get('href'))
            elif link.get('href').lower().endswith('.xlsx'):
                if 'data.gov.uk' in datalocation:
                    listxlsx.append(link.get('href'))
                else:
                    listxlsx.append('https://www.gov.uk/' + link.get('href'))
            elif link.get('href').lower().endswith('.xls'):
                if 'data.gov.uk' in datalocation:
                    listxls.append(link.get('href'))
                else:
                    listxls.append('https://www.gov.uk/' + link.get('href'))
            elif link.get('href').lower().endswith('.ods'):
                if 'data.gov.uk' in datalocation:
                    listods.append(link.get('href'))
                else:
                    listods.append('https://www.gov.uk/' + link.get('href'))
        if len([listcsvs, listxls, listxlsx, listods]) > 0:
            for filelocation in set(sum([listcsvs, listxls,
                                         listxlsx, listods], [])):
                if 'https://assets' in filelocation:
                    filelocation = filelocation.replace(
                        'https://www.gov.uk/', '')
                try:
                    breakout = 0
                    for exclusion in exclusions:
                        if exclusion in str(filelocation):
                            module_logger.info(
                                os.path.basename(filelocation) + ' is excluded! Verified problem.')
                            breakout = 1
                    if breakout == 1:
                        continue
                except Exception as e:
                    pass
                filename = os.path.basename(
                    filelocation).replace('?', '').lower()
                while filename[0].isalpha() is False:
                    filename = filename[1:]
                if ('gpc' not in filename.lower()) \
                        and ('procurement' not in filename.lower()) \
                        and ('card' not in filename.lower()):
                    if os.path.exists(os.path.join(filepath, department,
                                                   filename)) is False:
                        try:
                            r = requests.get(filelocation)
                            module_logger.info('File downloaded: ' +
                                               ntpath.basename(filelocation))
                            with open(os.path.join(
                                      os.path.join(filepath, department),
                                      filename), "wb") as csvfile:
                                csvfile.write(r.content)
                        except Exception as e:
                            module_logger.debug('Problem downloading ' +
                                                ntpath.basename(filelocation) +
                                                ': ' + str(e))
                        time.sleep(1.5)


def heading_replacer(columnlist, filepath):
    columnlist = [x if str(x) != 'nan' else 'dropme' for x in columnlist]
    columnlist = ['dropme' if str(x) == '-1.0' else x for x in columnlist]
    columnlist = [x if str(x) != '£' else 'amount' for x in columnlist]
    columnlist = [unidecode(x) if type(x) is str else x for x in columnlist]
    if ('Total Amount' in columnlist) and ('Amount' in columnlist):
        columnlist = ['dropme' if x ==
                      'Total Amount' else x for x in columnlist]
    if ('Gross' in columnlist) and ('Nett Amount' in columnlist):
        columnlist = ['Amount' if str(x) == 'Gross' else x for x in columnlist]
        columnlist = ['dropme' if str(x) ==
                      'Nett Amount' else x for x in columnlist]
    if ('Gross' in columnlist) and ('NET ' in columnlist):
        columnlist = ['Amount' if str(x) == 'Gross' else x for x in columnlist]
        columnlist = ['dropme' if str(x) ==
                      'NET ' else x for x in columnlist]
    if ('Gross' in columnlist) and ('Amount' not in columnlist):
        columnlist = ['Amount' if str(x) ==
                      'Gross' else x for x in columnlist]
    if ('Mix of Nett & Gross' in columnlist) and ('Amount' not in columnlist):
        columnlist = ['Amount' if str(x) ==
                      'Mix of Nett & Gross' else x for x in columnlist]

    columnlist = [
        'dropme' if 'departmentfamily' in str(x) else x for x in columnlist]
    columnlist = ['amount' if str(x) == '£' else x for x in columnlist]
    columnlist = [''.join(filter(str.isalpha, str(x).lower()))
                  for x in columnlist]
    replacedict = pd.read_csv(os.path.join(
        filepath, '..', '..', 'support', 'replacedict.csv'),
        header=None, dtype={0: str}).set_index(0).squeeze().to_dict()
    for item in range(len(columnlist)):
        for key, value in replacedict.items():
            if key == columnlist[item]:
                columnlist[item] = columnlist[item].replace(key, value)
    return columnlist


def parse_data(filepath, department, filestoskip=[]):
    allFiles = glob.glob(os.path.join(filepath, department, '*'))
    frame = pd.DataFrame()
    list_ = []
    filenames = []
    removefields = pd.read_csv(os.path.join(
        filepath, '..', '..', 'support', 'remfields.csv'),
        names=['replacement'])['replacement'].values.tolist()
    for file_ in allFiles:
        if ntpath.basename(file_).split('.')[0] not in filenames:
            filenames.append(ntpath.basename(file_).split('.')[0])
            try:
                if ntpath.basename(file_) in [x.lower() for x in filestoskip]:
                    module_logger.info(ntpath.basename(file_) +
                                       ' is excluded! Verified problem.')
                    continue
                if os.path.getsize(file_) == 0:
                    module_logger.info(ntpath.basename(
                        file_) + ' is 0b: skipping')
                    continue
                if file_.lower().endswith(tuple(['.csv', '.xls', '.xlsx', '.ods'])) is False:
                    module_logger.debug(ntpath.basename(file_) +
                                        ': not csv, xls, xlsx or ods: not parsing....')
                    continue
                try:
                    if (file_.lower().endswith('.xls')) or (file_.endswith('xlsx')):
                        df = pd.read_excel(file_, index_col=None, encoding='latin-1',
                                           header=None, error_bad_lines=False,
                                           skip_blank_lines=True, warn_bad_lines=False)
                    elif (file_.lower().endswith('.csv')):
                        df = pd.read_csv(file_, index_col=None, encoding='latin-1',
                                         header=None, error_bad_lines=False,
                                         skip_blank_lines=True, warn_bad_lines=False,
                                         engine='python')
                    elif (file_.lower().endswith('.ods')):
                        df = read_ods(file_)
                    if ntpath.basename(file_).lower() == 'dcms_transactions_over__25k_january_2016__1_.csv':
                        df.loc[-1] = ['Department family', 'Entity', 'Date',
                                      'Expense Type', 'Expense area', 'Supplier',
                                      'Transation number', 'Amount', 'Narrative']
                        df.index = df.index + 1  # shifting index
                        df = df.sort_index()
                    if len(df.columns) < 3:
                        if df.iloc[0].str.contains('!DOC').any():
                            module_logger.debug(ntpath.basename(
                                file_) + ': html. Delete.')
                        elif df.iloc[0].str.contains('no data', case=False).any():
                            module_logger.debug(ntpath.basename(file_)
                                                + ' has no data in it.')
                        else:
                            module_logger.debug(ntpath.basename(
                                file_) + ': not otherwise tab. ')
                        continue
                    if ntpath.basename(file_) == 'september_2013_publishable_spend_over__25k_csv.csv':
                        df.loc[0][5] = 'supplier'
                    while (((any("supplier" in str(s).lower() for s in list(df.iloc[0]))) is False)
                           and ((any("merchant" in str(s).lower() for s in list(df.iloc[0]))) is False)
                           and ((any("merchant name" in str(s).lower() for s in list(df.iloc[0]))) is False)
                           and ((any("supplier name" in str(s).lower() for s in list(df.iloc[0]))) is False)) \
                        or (((any("amount" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("total" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("gross" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("£" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("spend" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            #                    and ((any("sum of amount" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("mix of nett & gross" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("value" in str(s).lower() for s in list(df.iloc[0]))) is False)):
                        try:
                            df = df.iloc[1:]
                        except Exception as e:
                            module_logger.debug('Problem with trimming' +
                                                ntpath.basename(file_) +
                                                '. ' + str(e))
                    df.columns = heading_replacer(list(df.iloc[0]), filepath)
                    if len(df.columns.tolist()) != len(set(df.columns.tolist())):
                        df = df.loc[:, ~df.columns.duplicated()]
                    df = df.iloc[1:]
                    df.rename(columns=lambda x: x.strip(), inplace=True)
                    # drop empty rows and columns where half the cells are empty
                    df.dropna(thresh=4, axis=0, inplace=True)
                    df.dropna(thresh=0.75 * len(df), axis=1, inplace=True)
                    df['file'] = ntpath.basename(file_)
                    if department == 'dfeducation': #cut exec agencies here
                        try:
                            df = df[df['entity'] == 'DEPARTMENT FOR EDUCATION']
                        except Exception as e:
                            print('Whats going on here?' + e)
                    if list(df).count('amount') == 0 and list(df).count('gross') == 1:
                        df = df.rename(columns={'gross': 'amount'})
                    if list(df).count('amount') == 0 and list(df).count('grossvalue') == 1:
                        df = df.rename(columns={'grossvalue': 'amount'})
                    if len(df) > 0:
                        try:
                            df['amount'] = df['amount'].astype(str).str.replace(
                                ',', '').str.extract('(\d+)',
                                                     expand=False).astype(float)
                        except Exception as e:
                            module_logger.debug("Can't convert amount to float in " +
                                                ntpath.basename(file_) + '. ' +
                                                'Columns in this file ' +
                                                df.columns.tolist())
                        if df.empty is False:
                            list_.append(df)
                    else:
                        module_logger.info('No data in ' +
                                           ntpath.basename(file_) + '!')
                except Exception as e:
                    module_logger.debug('Problem with ' + ntpath.basename(file_) +
                                        ': ' + traceback.format_exc())
                    try:
                        module_logger.debug('The columns are: ' +
                                            str(df.columns.tolist()))
                    except ValueError:
                        pass
                    try:
                        module_logger.debug('The first row: ' + str(df.iloc[0]))
                    except ValueError:
                        pass
            except Exception as e:
                module_logger.debug('Something undetermined wrong with' +
                                    file_ + '. Heres the traceback: ' +
                                    str(e))
    frame = pd.concat(list_, sort=False)
    for column in frame.columns.tolist():
        if column.lower() in removefields:
            frame.drop([column], inplace=True, axis=1)
        if (column == ' ') or (column == ''):
            frame.drop([column], inplace=True, axis=1)
    #    frame = frame.drop_duplicates(keep='first', inplace=True)
    if 'nan' in list(frame):
        frame = frame.drop(labels=['nan'], axis=1)
    return frame


def createdir(filepath, dept):
    ''' check if the necessary subdirectory, and if not, make it'''
    if os.path.exists(os.path.join(filepath, dept)) is False:
        os.makedirs(os.path.join(filepath, dept))
    print('Working on ' + dept + '.')
    module_logger.info('Working on ' + dept + '.')


def dfeducation(filepath, dept):
    ''' Notes: collections with annual groupings, fairly clean.
    Notes: nb -- there is no page for 2018-2019 at present.
    How to update: look for early 2018 files in the collection page:
    /collections/dfe-department-and-executive-agency-spend-over-25-000
    Most recent file: oct 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        base1 = 'department-for-education-and-executive-agency-spend-over-25000'
        base2 = 'department-for-education'
        dataloc = [pubs + 'dfe-and-executive-agency-spend-over-25000-2018-to-2019',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2017-to-2018',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2016-to-2017',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2015-to-2016',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2014-to-2015',
                   pubs + base1 + '-financial-year-2013-to-2014',
                   pubs + '201213-' + base1,
                   pubs + 'departmental-and-alb-spend-over-25000-in-201112',
                   pubs + base2 + '-and-alb-spend-over-25000-201011']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip = ['dfe_spend_01apr_2012.csv',
                                       'dfe_spend_03jun_2012.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dohealth(filepath, dept):
    ''' Notes: collections with annual groupings, very logical annual subdomains
    Notes: However, department changes from DH to DHSC in end 2017 reshuffle.
    How to update: /collections/spending-over-25-000--2
    Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'dh-departmental-spend-over-25-000-2010',
                   pubs + 'dh-departmental-spend-over-25-000-2011',
                   pubs + 'dh-departmental-spend-over-25-000-2012',
                   pubs + 'dh-departmental-spend-over-25-000-2013',
                   pubs + 'dh-departmental-spend-over-25-000-2014',
                   pubs + 'dh-departmental-spend-over-25-000-2015',
                   pubs + 'dh-departmental-spend-over-25-000-2016',
                   pubs + 'dh-departmental-spend-over-25-000-2017',
                   pubs + 'dhsc-departmental-spending-over-25000-2018']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['december_18_over__25k_spend_data_to_be_published.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dftransport(filepath, dept):
    ''' Notes: everything in one data.gov.uk page, easy to scrape
    How to update: check dataset/financial-transactions-data-dft
    Most recent file: may 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/financial-transactions-data-dft']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['dft-monthly-transparency-data-may-2017.csv',
                                     'dft-monthly-spend-201409.csv',
                                     'dft-monthly-transparency-data-september-2018.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def cabinetoffice(filepath, dept):
    ''' Notes: everything in one publications page
    How to update: publications/cabinet-office-spend-data
    Most recent file: dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'cabinet-office-spend-data']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dfintdev(filepath, dept):
    ''' Notes: everything in one data.gov.uk page, easy. Threshold £500
    How to update: check dataset/financial-transactions-data-dft
    Most recent file: Jan 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/spend-transactions-by-dfid']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=['feb2015.csv',
                                                     'January2014.csv',
                                                     'may-2016.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dfinttrade(filepath, dept):
    ''' Notes: Dept only created in 2016. Groupled publications in collection
        for the most part of the first two years, then buggy/bad individual
        files hyperlinked to the collection page.
        How to update: check /collections/dit-departmental-spending-over-25000
        Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'dit-spending-over-25000-december-2018',
                   pubs + 'dit-spending-over-25000-november-2018',
                   pubs + 'dit-spending-over-25000-october-2018',
                   pubs + 'dit-spending-over-25000-september-2018',
                   pubs + 'dit-spending-over-25000-august-2018',
                   pubs + 'dit-spending-over-25000-july-2018',
                   pubs + 'dit-spending-over-25000-june-2018',
                   pubs + 'dit-spending-over-25000-may-2018',
                   pubs + 'dit-spending-over-25000-april-2018',
                   pubs + 'dit-spending-over-25000-march-2018',
                   pubs + 'dit-spending-over-25000-february-2018',
                   pubs + 'dit-spending-over-25000-january-2018',
                   pubs + 'dit-spending-over-25000-december-2017',
                   pubs + 'dit-spending-over-25000-november-2017',
                   pubs + 'department-for-international-trade-spend-2017-to-2018',
                   pubs + 'department-for-international-trade-spend-2016-to-2017']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dworkpen(filepath, dept):
    ''' Notes: Groupled publications in collection, but ends half through 2017
        How to update: check page at collections/dwp-payments-over-25-000
        Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/' + 'ccdc397a-3984-453b-a9d7-e285074bba4d/' +
                   'spend-over-25-000-in-the-department-for-work-and-pensions']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def modef(filepath, dept):
    '''Notes: grouped £500 and £25000 together in one collection: super great.
    How to update: check collections/mod-finance-transparency-dataset
    Most recent file: Jan 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'mod-spending-over-25000-january-to-december-2014',
                   pubs + 'mod-spending-over-25000-january-to-december-2015',
                   pubs + 'mod-spending-over-25000-january-to-december-2016',
                   pubs + 'mod-spending-over-25000-january-to-december-2017',
                   pubs + 'mod-spending-over-25000-january-to-december-2018',
                   pubs + 'mod-spending-over-25000-january-to-december-2019']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def mojust(filepath, dept):
    '''Notes: because there are so many MOJ arms length bodies, the collections
    are a bit weird. Slightly outdated alsoself.
    How to update: search for a new landing page, something akin to:
    collections/moj-spend-over-25000-2018?
    Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'spend-over-25-000',  # really old data
                   pubs + 'ministry-of-justice-spend-over-25000-2013',
                   pubs + 'ministry-of-justice-spend-over-25000-2014',
                   pubs + 'ministry-of-justice-spend-over-25000-2015',
                   pubs + 'ministry-of-justice-spend-over-25000-2016',
                   pubs + 'ministry-of-justice-spend-over-25000-2017',
                   pubs + 'ministry-of-justice-spending-over-25000-2018']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dcultmedsport(filepath, dept):
    ''' Notes: This is quite a mess -- each pubs page has a differet annual set
    How to update: search for a new landing page, something akin to:
    publications/dcms-transactions-over-25000-201819?
    Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'transactions-over-25k-2013-2014',
                   pubs + 'transactions-over-25000-august-2014',
                   pubs + 'transactions-over-25000-july-2014',
                   pubs + 'dcms-transactions-over-25000-2014-15',
                   pubs + 'dcms-transactions-over-25000-2015-16',
                   pubs + 'dcms-transactions-over-25000-201617',
                   pubs + 'dcms-transactions-over-25000-201718',
                   pubs + 'dcms-transactions-over-25000-201819']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def ukexpfin(filepath, dept):
    ''' Notes: random files appear to be missing? good collection structure
    How to update: should be automatic? collections/ukef-spend-over-25-000
    Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/ukef-spend-over-25-000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dbusenind(filepath, dept):
    ''' Notes: very nice collection structure all on one pageself.
    How to update: collections/beis-spending-over-25000
    Most recent file: Sept 2017 (STILL! as of March 2019)'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/beis-spending-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dfeeu(filepath, dept):
    ''' Notes: this is a bit of a mess... need to add files one by one?
    Most recent file: January 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        lander = 'department-for-exiting-the-european-union-spend-over-25000-'
        dataloc = [pubs + lander + 'november-2017-to-february-2018',
                   pubs + lander + 'march-2018',
                   pubs + lander + 'april-2018',
                   pubs + lander + 'may-2018',
                   pubs + lander + 'june-2018',
                   pubs + lander + 'july-2018',
                   pubs + lander + 'august-2018',
                   pubs + lander + 'september-2018',
                   pubs + lander + 'october-2018',
                   pubs + lander + 'november-2018',
                   pubs + lander + 'december-2018',
                   pubs + lander + 'january-2019']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip = ['dcms_transactions_over__25k_january_2016__1_'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def foroff(filepath, dept):
    ''' Note: great: everything in one collection pageself.
    How to update: should be automatic, but if not check:
    collections/foreign-office-spend-over-25000
    Most recent file: Jan 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/foreign-office-spend-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['Publishable_November_2014_Spend.csv',
                                     'october_2013.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def hmtreas(filepath, dept):
    ''' Note: out of date a bit, but collection is clean
    How to update: collections/25000-spend
    Most recent file: March 2017
    Note: No more recent file as of Sept 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/25000-spend')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def mhclg(filepath, dept):
    ''' Note this changes from dclg in dec 2017.
    Note: therefore, grab all MHCLG only... this is a broken mess in general
    How to update: collections/mhclg-departmental-spending-over-250
    Most recent file: jan 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(
            base + 'collections/mhclg-departmental-spending-over-250')
        # r1 = requests.get(base + 'collections/dclg-spending-over-250')
        htmllist = re.findall(
            "ment/publications/(.*?)\" data-track", r.text)  # + \
        # re.findall("ment/publications/(.*?)\" data-track", r1.text)
        htmllist = [x for x in htmllist if ("procurement" not in x) and
                    ("card" not in x) and ("card" not in x)]
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def nioff(filepath, dept):
    '''Note: this data seems really, really out of date. Nothing on data.gov.uk
    How to update: publications/nio-transaction-spend-data-july-2011
    Most recent file: July 2011?
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/expenditure-over-25-000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def waleoff():
    print('Wales Office only has Procurement Card data: skip for now')


def leadercommons():
    print('No data for Office of the Leader of the House of Commons')


def leaderlords():
    print('No data for Office of the Leader of the House of Lords')


def scotoff(filepath, dept):
    '''Notes: this is really grim. have to manually scrape pages from the
    search function (eurgh)...
    How to update: manual search https://www.gov.uk/government/publications ?
    Most recent file: Feb 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        land = 'departmental-spend-over-25000'
        SO = 'scotland-office-'
        scoto = 'office-of-the-secretary-of-state-for-scotland-'
        htmllist = [land + '-january-2018',
                    land + '-february-2018',
                    land + '-march-2018',
                    land + '-april-2018',
                    land + '-may-2018',
                    land + '-june-2018',
                    land + '-july-2018',
                    land + '-august-2018',
                    land + '-september-2018',
                    scoto + land + '-october-2018',
                    scoto + land + '-november-2018',
                    scoto + land + '-december-2018',
                    scoto + land + 'january-2019',
                    scoto + land + '-february-2019',
                    land + '-november-2017',
                    land + '-december-2017',
                    land + '-october-2017',
                    land + '-august-2017',
                    land + '-september-2017',
                    land + '-april-2017',
                    land + '-march-2017',
                    dept + '-february-2017',
                    SO + land + '-july-2017',
                    SO + land + '-may-2017',
                    SO + land + '-may-2017',
                    SO + land + '-june-2017',
                    land + '-may-2016',
                    land + '-february-2016',
                    land + '-march-2016',
                    land + '-april-2016',
                    land + '-july-2016',
                    land + '-september-2016',
                    land + '-november-2016',
                    land + '-january-2017',
                    land + '-june-2016',
                    land + '-december-2016',
                    land + '-october-2016',
                    land + '-december-2015',
                    land + '-april-2014--2',
                    land + '-march-2014--2',
                    'departmental-spend-over-25-000-december-2012',
                    'department-spend-over-25-000-august-2011']
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
        r = requests.get(
            base + 'collections/scotland-office-spend-over-25-000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['January__2018_-_Transparency-SO.csv',
                                     'Departmental_spend_over__25_000_January_2017.csv',
                                     'AUGUST_2011_20SO_20Transparency.csv',
                                     'June_2017_-_Transparency_-_SO.csv',
                                     'October_2017_-_Transparency-SO.csv',
                                     'Spend-Over-25k-Jan-2012.csv',
                                     'April_2014_transparency_OAG.csv',
                                     'Februry__2018_-_Transparency-SO.csv',
                                     'july-september_16_senor_officials__travel_q2.csv',
                                     'july-september_16_senior_officials_hospitality.csv',
                                     'transparency_sodecember18.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def gldagohmcpsi(filepath, dept):
    ''' Notes: Data for Government Legal Department, Attorney General’s Office
    and HM Crown Prosecution Service Inspectorate. The final link on the
    page is everything prior to march 2017
    How to update: collections/gld-ago-hmcpsi-transactions-greater-than-25000
    Most recent file: August 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/' +
                         'gld-ago-hmcpsi-transactions-greater-than-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def homeoffice(filepath, dept):
    '''Note: all there, but gotta go into each annual page, not updated recently
    How to update: publications/home-office-spending-over-25000-2018 ?
    Most recent file: Jan 2017.
    Interesting -- probably dont expect anything off them any time soon:
    https://ico.org.uk/media/action-weve-taken/decision-notices/2018/2258292/fs50694249.pdf
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'home-office-spending-over-25000-2017',
                   pubs + 'home-office-spending-over-25000-2016',
                   pubs + 'home-office-spending-over-25000-2015',
                   pubs + 'home-office-spending-over-25000-2014',
                   pubs + 'transparency-spend-over-25-000']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=['april-2011.xls',
                                                     'HO_GLAA_25K_Spend_2018_ODS.ods',
                                                     'HO_GLAA_25K_Spend_2018_CSV.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def oags(filepath, dept):
    '''Note: this is a mess. old files are part of a big collection, but new
    files are randomly scattered across the pubs subdomain? Some random files
    are also missing.
    How to update: manual search https://www.gov.uk/government/publications ?
    Most recent file: Feb 2018?
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        land = 'oag-spending-over-25000-for-'
        htmllist = [land + 'september-2018',
                    land + 'august-2018',
                    land + 'july-2018',
                    land + 'june-2018',
                    land + 'may-2018',
                    land + 'april-2018',
                    land + 'march-2018',
                    land + 'february-2018',
                    land + 'january-2018',
                    land + 'december-2017',
                    land + 'november-2017',
                    land + 'october-2017',
                    land + 'september-2017',
                    land + 'august-2017',
                    land + 'july-2017',
                    land + 'june-2017',
                    land + 'may-2017',
                    land + 'april-2017',
                    land + 'march-2017',
                    land + 'february-2017',
                    land + 'january-2017',
                    land + 'december-2016']
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
        r = requests.get(base + 'collections/spend-over-25-000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath,
                        dept,
                        filestoskip=['March_2017_-_Transparency_OAG.csv',
                                     'August_2016_-_Transparency-OAG.csv',
                                     'May_2013_OAG.csv',
                                     'Spend_over_25K_February_2013.xls',
                                     'May_2014_transparency_OAG.csv',
                                     'April_2014_transparency_OAG.csv',
                                     'Transparency-April-2013.csv',
                                     'Spend_over_25k_Apr_2011_-_Mar_2012.xls',
                                     'Spend_over_25k_August_2012.xls',
                                     'October_2017_-_Transparency_-_OAG.csv',
                                     'Nov_2014_transparency_OAG.csv',
                                     'Spend_over_25k_May_2012.xls',
                                     'Spend_over_25k_October_2012.xls',
                                     'August_2015_-_OAG.csv',
                                     'transparency_oag_aug_18.xlsx',
                                     'July_2015_-_Transparency_-_OAG.csv',
                                     'OAG_Transparency_Aug_2013.csv',
                                     'Nov_2015_-_Transparency-OAG.csv',
                                     'OAG_transparency_Sep_2013.csv',
                                     'July_2014_transparency_OAG.csv',
                                     'Sept_2016_-_Transparency-OAG.csv',
                                     'April_2015_-_Transparency-OAG.csv',
                                     'November_2017_-_Transparency_-_OAG.csv',
                                     'Sept_2015_-_Transparency-OAG.csv',
                                     'OAG_Transparency_Oct_2013.csv',
                                     'July_2017_-_Transparency_-_OAG.csv',
                                     'Spend_over_25k_July_2012.xls',
                                     'Jan_2016_-_Transparency-OAG.csv',
                                     'Spend_over_25k_June_2012.xls',
                                     'February_2017_-_Transparency_OAG.csv',
                                     'Transparency_March_2013.csv',
                                     'Transparency_-_OAG.csv',
                                     'Feb_2016_-_Transparency-OAG.csv',
                                     'Aug_2013_OAG_Transparency.csv',
                                     'September_2017_-_Transparency_-_OAG.csv',
                                     'December_2017_-_Transparency_-_OAG.csv',
                                     'Feb_2014_transparency_OAG.csv',
                                     'October_2014_-_Transparency_Report_-_Expenses.csv',
                                     'Aug_2014_transparency_OAG.csv',
                                     'June_2017_-_Transparency_-_OAG.csv',
                                     'Spend_over_25k_September_2012.xls',
                                     'June_2015_-_Transparency_-_OAG.csv',
                                     'June_2014_transparency_OAG.csv',
                                     'May_2016_-_Transparency-OAG.csv',
                                     'December_2016_-_Transparency_OAG.csv',
                                     'Spend_over_25k_November_2012.xls',
                                     'Dec_2013_Transparency.xlsx',
                                     'Nov_2013_Transparency.xlsx',
                                     'Dec_2014_Transparency_OAG.xlsx'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def defra(filepath, dept):
    ''' Note: complete listing on data.gov.uk.
    How to update: automatic?: dataset/financial-transactions-data-defra
    Most recent file: January 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        key = '91072f06-093a-41a2-b8b5-6f120ceafd62'
        landingpage1 = '/spend-over-25-000-in-the'
        landingpage2 = '-department-for-environment-food-and-rural-affairs'
        dataloc = [data + 'dataset/' + key + landingpage1 + landingpage2]
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def charcom(filepath, dept):
    ''' great: have to manually find pages in the search, slightly out of date
    How to update: manual search https://www.gov.uk/government/publications ?
    Most recent file: March 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        base1 = 'invoices-over-25k-during-financial-year-'
        base2 = 'charity-commission-spend-over-25000-'
        dataloc = [pubs + base1 + '2011-2012', pubs + base1 + '2012-2013',
                   pubs + base1 + '2013-2014', pubs + base1 + '2014-15',
                   pubs + base1 + '2015-16', pubs + base1 + '2016-2017',
                   pubs + base2 + '2017-2018', pubs + base2 + '2018-2019']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=['May11.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def commarauth(filepath, dept):
    ''' this is a collection, so should update automatically
        Most recent file: Dec 2018
    '''

    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/cma-spend-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['Payments_over__25k_October_17.ods',
                                     'spend-over-25k-june-2017.ods',
                                     'Payments_over__25k_September_17.ods',
                                     'spend-over-25k-may-2017.ods'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)
    # https://stackoverflow.com/questions/17834995/how-to-convert-opendocument-spreadsheets-to-a-pandas-dataframe


def crownprosser(filepath, dept):
    ''' Note: There is now a dedicated page when there wasnt previously
    Check that this actually works on the next runself, as it may be trying
    to parse really janky stuff (there are multiple files on the page...)

    Last update:
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        key = '22c9d6a0-2139-46c4-bf3d-5fe9722de873/'
        landingpage = 'spend-over-25-000-in-the-crown-prosecution-service'
        dataloc = [data + 'dataset/' + key + landingpage]
        get_data(dataloc, filepath, dept, exclusions=[])
    try:
        df = parse_data(filepath, dept,
                        filestoskip=[''])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def fsa(filepath, dept):
    ''' some of the files requested are links to other sites and are returning
    html: but these dont get parsed so just ignore them for now,

    Last update: October 2018'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        fsagit = 'https://github.com/'
        subdir = 'fsadata/spend-over-25-000-in-the-food-standards-agency/'
        base1 = 'https://www.food.gov.uk/about-us/data-and-policies/'
        subdir1 = 'transparencydata/expenditureover25k/'
        datalocs = [fsagit + subdir + 'tree/gh-pages/data',
                    base1 + subdir1 + 'expenditure-over-25k-april-2015-march-2016',
                    base1 + subdir1 + 'fsa-expenditure-over25k-2014-2015',
                    base1 + subdir1 + 'expenditure25k-apr-2013-mar-2014',
                    base1 + subdir1 + 'expenditure25k-apr-2012-mar-2013',
                    base1 + subdir1 + 'expenditure25k-apr-2011-mar-2012',
                    base1 + subdir1 + 'expenditure25k-apr-2010-mar-2011']
        for dataloc in set(datalocs):
            r = requests.get(dataloc)
            if 'github' in dataloc:
                listcsvs = re.findall(
                    '\" href=\"/fsadata/spend-over-25-000-in-the-food-standards-agency/blob/gh-pages/data/(.*?)\"', r.text)
                listcsvs = [
                    'https://raw.githubusercontent.com/fsadata/spend-over-25-000-in-the-food-standards-agency/gh-pages/data/' + s for s in listcsvs]
            else:
                soup = BeautifulSoup(r.text, 'lxml')
                listcsvs = []
                for link in soup.findAll('a', attrs={'href': re.compile("^https://")}):
                    listcsvs.append(link.get('href'))
            if len(listcsvs) > 0:
                for fileloc in listcsvs:
                    filename = os.path.basename(fileloc).lower()
                    if ('gpc' not in filename.lower() and
                            '.csv' in filename.lower()):
                        if os.path.exists(os.path.join(
                                os.path.join(filepath, dept),
                                filename)) is False:
                            try:
                                r = requests.get(fileloc)
                                module_logger.info('File downloaded: ' +
                                                   ntpath.basename(fileloc))
                                with open(os.path.join(filepath, dept,
                                                       filename),
                                          "wb") as csvfile:
                                    csvfile.write(r.content)
                            except Exception as e:
                                module_logger.debug('Problem downloading ' +
                                                    ntpath.basename(fileloc) +
                                                    ': ' + str(e))
                        time.sleep(1.5)
    try:
        df = parse_data(filepath, dept, filestoskip=['fsa-spend-aug2013.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def forcomm(filepath, dept):
    ''' Fixed August 2018 to instead go direct to forestry.gov and get data
    from there. A few of the files are duplicated across months but they
    appear to be for different things, i.e.
        April 2018 Forestry Commisson England (CSV File)
        April 2018 Forest Enterprise England (CSV File)
    but these are just executuve agencies?
    last updated: June 2018'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        homepage = 'https://www.forestry.gov.uk'
        r = requests.get(homepage +
                         '/website/forestry.nsf/byunique/infd-8apcws')
        listcsvs = [homepage +
                    x for x in re.findall('<li><a href="(.*?.csv)"', r.text)]
        for fileloc in set(listcsvs):
            filename = fileloc.split('/')[-1]
            try:
                if os.path.exists(os.path.join(filepath, dept,
                                               filename)) is False:
                    if filename != 'June2016Over25k.csv':
                        r = requests.get(fileloc)
                        module_logger.info('File downloaded: ' +
                                           ntpath.basename(fileloc))
                        with open(os.path.join(filepath, dept, filename),
                                  "wb") as csvfile:
                            csvfile.write(r.content)
                        time.sleep(1.5)
            except Exception as e:
                module_logger.debug('Problem downloading ' +
                                    ntpath.basename(fileloc) +
                                    ': ' + str(e))
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def govlegdep():
    print('Data for GLD is merged with the AGO.')


def govaccdept(filepath, dept):
    ''' Note: each year has its own publications page, pretty janky
    How to update: search for 'gad-spend-greater-than-25000-2018?'?
    Most recent file: Jan 2019'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'gad-spend-greater-than-25000-2014',
                   pubs + 'gad-spend-greater-than-25000-2015',
                   pubs + 'gad-spend-greater-than-25000-2016',
                   pubs + 'gad-spend-greater-than-25000-2017',
                   pubs + 'gad-spend-greater-than-25000-2018',
                   pubs + 'gad-spend-greater-than-25000-2018',
                   pubs + 'gad-spend-greater-than-25000-2019']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=['GAD_Nov_2016__25k_.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def hmlandreg(filepath, dept):
    ''' Notes: exemplary! all in one collections page.
    How to update: should be automatic, if not, check:
    collections/land-registry-expenditure-over-25000
    Most recent file: November 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(
            base + 'collections/land-registry-expenditure-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def hmrc(filepath, dept):
    ''' Notes: exemplary! all in one collections page.
    How to update: should be automatic, if not, check:
    collections/spending-over-25-000
    Most recent file: Jan 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/spending-over-25-000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage],
                     filepath, dept, exclusions=['RCDTS'])
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def natsavinv(filepath, dept):
    '''Note: have to devise a less general function to visit third party website
    How to update: should be automatic, if not check the main landing page
    Most recent file: February 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        domain = 'https://nsandi-corporate.com'
        r = requests.get(domain + '/performance/transparency')
        htmllist = re.findall('<a href="/sites/default/files/(.*?)"', r.text)
        for file_ in set(htmllist):
            if '.csv' in file_:
                file_ = domain + '/sites/default/files/' + file_
                if os.path.exists(os.path.join(os.path.join(filepath, dept),
                                               file_.split('/')[-1].lower())) is False:
                    try:
                        if file_.lower().contains('transparency-25k-2014-dec.csv') is false:
                            r = requests.get(file_)
                            with open(os.path.join(
                                      os.path.join(filepath, dept),
                                      file_.split('/')[-1].lower()), "wb") as csvfile:
                                csvfile.write(r.content)
                            module_logger.info('File downloaded: ' +
                                               ntpath.basename(file_))
                            time.sleep(1.5)
                    except Exception as e:
                        module_logger.debug('Problem downloading ' +
                                            ntpath.basename(file_) +
                                            ': ' + str(e))
    try:
        df = parse_data(filepath, dept, filestoskip=[
            'transparency-25k-12-2014.csv',
            'transparency-25k-2014-dec.csv',
            'transparency-25k-2014-Dec.csv',
            ''])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def natarch(filepath, dept):
    ''' Note: complete listing on data.gov.uk.
    How to update: automatic?: dataset/national-archives-items-of-spending
    Most recent file: Feb 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/national-archives-items-of-spending']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=[
            'april2013-spend-over10k.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def natcrimag():
    print('The National Crime Agency is exempt from FOI stuffs.')


def offrailroad(filepath, dept):
    ''' Note: up to date listing on data.gov.uk, but starts late?
    How to update: dataset/office-of-rail-and-road-spending-over-25000-dataset
    Most recent file: Jan 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        landingpage = 'dataset/office-of-rail-and-road-spending-over-25000-dataset'
        dataloc = [data + landingpage]
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def ofgem(filepath, dept):
    ''' Note: custom function devised by the ofgem search function
    How to update: maybe add an extra page onto the range?
    Last update: April 2018?
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        ofgembase = 'https://www.ofgem.gov.uk/about-us/transparency/'
        search = 'expenses-and-expenditure/payments-suppliers?page='
        dataloc = []
        for page in range(0, 10):
            try:
                r = requests.get(ofgembase + search + str(page))
                dataloc = dataloc + \
                    re.findall('field-content"><a href="(.*?)"><span>', r.text)
            except Exception as e:
                module_logger.info(dept + 'website down?')
                break
            time.sleep(1)
    if 'noscrape' not in sys.argv:
        for file_ in set(dataloc):
            r = requests.get('https://www.ofgem.gov.uk/' + file_)
            time.sleep(1.5)
            files = re.findall(
                'file-container"><a href=\"(.*?)\" onclick=\"', r.text)
            for csv_ in files:
                if '.csv' in csv_:
                    if os.path.exists(os.path.join(filepath, dept,
                                                   csv_.split('/')[-1])) is False:
                        try:
                            r = requests.get(csv_)
                            with open(os.path.join(
                                      os.path.join(filepath, dept),
                                      csv_.split('/')[-1]), "wb") as csvfile:
                                csvfile.write(r.content)
                            module_logger.info('File downloaded: ' +
                                               ntpath.basename(csv_))
                            time.sleep(1.5)
                        except Exception as e:
                            module_logger.debug('Problem downloading ' +
                                                ntpath.basename(csv_))
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def ofqual(filepath, dept):
    '''Notes: Everything in one publications sheet
    How to update: automatic? publications/ofqual-spend-data-over-500
    Most recent file: 2018 to 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'ofqual-spend-data-over-500']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['Ofqual_Expenditure_over_25k_May_2016.csv',
                                     'Ofqual_Expenditure_over_25k_January_2017.csv',
                                     'Ofqual_Expenditure_over_25k_November_2016.csv',
                                     'Ofqual_Expenditure_over_25k_July_2013.csv',
                                     'Ofqual_Expenditure_over_25k_October_2016.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def ofsted(filepath, dept):
    '''Notes: four publications pages linked together via a collection
    How to update: collections/ofsted-spending-over-25000
    Most recent file: January 2019'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'ofsted-spending-over-25000-since-april-2010',
                   pubs + 'ofsted-spending-over-25000-2019',
                   pubs + 'ofsted-spending-over-25000-2016',
                   pubs + 'ofsted-spending-over-25000-2017',
                   pubs + 'ofsted-spending-over-25000-2018']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def serfraud(filepath, dept):
    ''' Custom site, but looks ok, maybe one day will have to add to the range:
    Last file: Sept 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        serfrau1 = 'https://www.sfo.gov.uk/'
        serfrau2 = 'publications/corporate-information'
        serfrau3 = '/transparency/procurement-spend-over-25000/'
        search = '?cp_procurement-spend-over-25000='
        dataloc = []
        for page in range(1, 3):
            try:
                r = requests.get(serfrau1 + serfrau2 +
                                 serfrau3 + search + str(page))
                soup = BeautifulSoup(r.text, "lxml")
                for link in soup.findAll('a'):
                    if 'download/procurement' in str(link.get('href')):
                        dataloc.append(str(link.get('href')))
            except Exception as e:
                module_logger .info(dept + 'website down?')
                break
            time.sleep(1)
        for file_ in set(dataloc):
            if file_ not in ['wpdmdl=2050',  # these are zipped xml files?
                             'wpdmdl=2193',
                             'wpdmdl=3229',
                             'wpdmdl=6819',
                             'wpdmdl=6825']:
                try:
                    if os.path.exists(
                        os.path.join(filepath, dept,
                                     file_.split('/')[-1].replace('?', '') + '.csv')) is False:
                        r = requests.get(serfrau1 + 'download/' + file_)
                        with open(os.path.join(filepath, dept,
                                               file_.split('/')[-1].replace('?', '') + '.csv'),
                                  "wb") as csvfile:
                            csvfile.write(r.content)
                        module_logger.info('File downloaded: ' +
                                           ntpath.basename(file_).replace('?', ''))
                        time.sleep(1.5)
                except Exception as e:
                    module_logger.debug('Problem downloading ' +
                                        ntpath.basename(file_).replace('?', '') +
                                        ': ' + str(e))
    try:
        df = parse_data(filepath, dept, filestoskip=['wpdmdl=2193.csv',
                                                     '?wpdmdl=2050.csv',
                                                     'wpdmdl=6819.csv',
                                                     'wpdmdl=6825.csv',
                                                     'wpdmdl=2050.csv',
                                                     'wpdmdl=20440.csv',
                                                     'wpdmdl=3229.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def supcourt(filepath, dept):
    '''Notes: have to go via a third party website and create a custom function
    but otherwise seems to be working fine and well. Nicely grouped annually.
    How to update: check for existance of 2020.csv at:
        https://www.supremecourt.uk/about/transparency.html
    Most recent file: annual 2019 file -- specifies when most recently updated
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        SCbase = 'https://www.supremecourt.uk/docs/transparency-transactions-'
        htmllist = [SCbase + '2010.csv', SCbase + '2011.csv',
                    SCbase + '2012.csv', SCbase + '2013.csv',
                    SCbase + '2014.csv', SCbase + '2015.csv',
                    SCbase + '2016.csv', SCbase + '2017.csv',
                    SCbase + '2018.xlsx', SCbase + '2019.xlsx']
        for html_ in set(htmllist):
            try:
                if os.path.exists(os.path.join(filepath, dept,
                                               html_.split('/')[-1])) is False:
                    r = requests.get(html_)
                    with open(os.path.join(os.path.join(filepath, dept),
                                           html_.split('/')[-1]),
                              "wb") as csvfile:
                        csvfile.write(r.content)
                        module_logger.info('File downloaded: ' +
                                           ntpath.basename(html_))
            except Exception as e:
                module_logger.debug('Problem downloading ' +
                                    ntpath.basename(html_) +
                                    ': ' + str(e))
            time.sleep(1)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def ukstatauth():
    print('Cannot find data for the UK Statistics Authority...')


def ofwat(filepath, dept):
    ''' Note: complete listing on data.gov.uk. some deadlinks...
    How to update: automatic?: dataset/financial-transactions-data-ofwat
    Most recent file:December 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        key = '43e2236d-f00a-4762-929d-2211e0ab5ad8/'
        landingpage = 'spend-over-25-000-in-ofwat'
        dataloc = [data + 'dataset/' + key + landingpage]
        get_data(dataloc, filepath, dept, exclusions=[
                 'prs_dat_transactions201107'])
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['prs_dat_transactions201008.csv',
                                     'prs_dat_transactions201208.csv',
                                     'prs_dat_transactions201110.csv',
                                     'prs_dat_transactions201206.csv',
                                     'prs_dat_transactions201501.csv',
                                     'prs_dat_transactions201202.csv',
                                     'prs_dat_transactions201410.csv',
                                     'prs_dat_transactions201503.csv',
                                     'prs_dat_transactions201412.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def build_merged(rawpath):
    ''' build merged databases'''
    print('\n>> Now working on Constructing Merged Departments!\n')
    filecountstart = sum([len(files) for r, d, files in os.walk(rawpath)])
    if 'depttype=nonministerial' not in sys.argv:
        modef(os.path.join(rawpath, 'ministerial'), 'modef')
        cabinetoffice(os.path.join(rawpath, 'ministerial'), 'cabinetoffice')
        dftransport(os.path.join(rawpath, 'ministerial'), 'dftransport')
        dohealth(os.path.join(rawpath, 'ministerial'), 'dohealth')
        dfeducation(os.path.join(rawpath, 'ministerial'), 'dfeducation')
        dfintdev(os.path.join(rawpath, 'ministerial'), 'dfintdev')
        dfinttrade(os.path.join(rawpath, 'ministerial'), 'dfinttrade')
        dworkpen(os.path.join(rawpath, 'ministerial'), 'dworkpen')
        mojust(os.path.join(rawpath, 'ministerial'), 'mojust')
        dcultmedsport(os.path.join(rawpath, 'ministerial'), 'dcultmedsport')
        ukexpfin(os.path.join(rawpath, 'ministerial'), 'ukexpfin')
        dbusenind(os.path.join(rawpath, 'ministerial'), 'dbusenind')
        dfeeu(os.path.join(rawpath, 'ministerial'), 'dfeeu')
        foroff(os.path.join(rawpath, 'ministerial'), 'foroff')
        hmtreas(os.path.join(rawpath, 'ministerial'), 'hmtreas')
        mhclg(os.path.join(rawpath, 'ministerial'), 'mhclg')
        nioff(os.path.join(rawpath, 'ministerial'), 'nioff')
        waleoff()
        scotoff(os.path.join(rawpath, 'ministerial'), 'scotoff')
        gldagohmcpsi(os.path.join(rawpath, 'ministerial'), 'gldagohmcpsi')
        homeoffice(os.path.join(rawpath, 'ministerial'), 'homeoffice')
        leaderlords()
        leadercommons()
        oags(os.path.join(rawpath, 'ministerial'), 'oags')
        defra(os.path.join(rawpath, 'ministerial'), 'defra')
    if 'depttype=ministerial' not in sys.argv:
        charcom(os.path.join(rawpath, 'nonministerial'), 'charcom')
        commarauth(os.path.join(rawpath, 'nonministerial'), 'commarauth')
        crownprosser(os.path.join(rawpath, 'nonministerial'), 'crownprosser')
        fsa(os.path.join(rawpath, 'nonministerial'), 'fsa')
        forcomm(os.path.join(rawpath, 'nonministerial'), 'forcomm')
        govlegdep()
        govaccdept(os.path.join(rawpath, 'nonministerial'), 'govaccdept')
        hmlandreg(os.path.join(rawpath, 'nonministerial'), 'hmlandreg')
        hmrc(os.path.join(rawpath, 'nonministerial'), 'hmrc')
        natsavinv(os.path.join(rawpath, 'nonministerial'), 'natsavinv')
        natarch(os.path.join(rawpath, 'nonministerial'), 'natarch')
        natcrimag()
        offrailroad(os.path.join(rawpath, 'nonministerial'), 'offrailroad')
        ofgem(os.path.join(rawpath, 'nonministerial'), 'ofgem')
        ofqual(os.path.join(rawpath, 'nonministerial'), 'ofqual')
        ofsted(os.path.join(rawpath, 'nonministerial'), 'ofsted')
        serfraud(os.path.join(rawpath, 'nonministerial'), 'serfraud')
        supcourt(os.path.join(rawpath, 'nonministerial'), 'supcourt')
        ukstatauth()
        ofwat(os.path.join(rawpath, 'nonministerial'), 'ofwat')
    filecountend = sum([len(files) for r, d, files in os.walk(rawpath)])
    print('Added a total of ' + str(filecountend - filecountstart) +
          ' new files.')
