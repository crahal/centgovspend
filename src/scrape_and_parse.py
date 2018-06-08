import os
import traceback
import requests
import time
import glob
import re
from bs4 import BeautifulSoup
import ntpath
import sys
from unidecode import unidecode
import pandas as pd
import logging
module_logger = logging.getLogger('centgovspend_application')
base = 'https://www.gov.uk/government/'
pubs = base + 'publications/'
data = 'https://data.gov.uk/'


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
    frame = pd.concat(list_)
    frame.dropna(thresh=0.90 * len(df), axis=1, inplace=True)
    return frame


def get_data(datalocations, filepath, department, exclusions=[]):
    ''' send data.gov.uk or gov.uk data through here.
    If the data is hosted on a departments specific site, write it in
    main.src
    '''
    for datalocation in datalocations:
        r = requests.get(datalocation)
        if 'data.gov.uk' in datalocation:
            listcsvs = re.findall(
                'contentUrl\":\"(.*?)\",\"fileFormat\":\"csv\"', r.text)
            listCSVs = re.findall(
                'contentUrl\":\"(.*?)\",\"fileFormat\":\"CSV\"', r.text)
            listcsvs = listCSVs + listcsvs
            listxls = re.findall(
                'contentUrl\":\"(.*?)\",\"fileFormat\":\"xls\"', r.text)
            listXLS = re.findall(
                'contentUrl\":\"(.*?)\",\"fileFormat\":\"XLS\"', r.text)
            listxls = listXLS + listxls
            listxlsx = re.findall(
                'contentUrl\":\"(.*?)\",\"fileFormat\":\"xlsx\"', r.text)
            listXLSX = re.findall(
                'contentUrl\":\"(.*?)\",\"fileFormat\":\"XLSX\"', r.text)
            listxlsx = listXLSX + listxlsx
        else:
            listcsvs = ['https://www.gov.uk/' + x for x in
                        re.findall('<a href="(.*?.csv)"', r.text)]
            listxls = ['https://www.gov.uk/' + x for x in
                       re.findall('href="(.*?.xls)"', r.text)]
            listxlsx = ['https://www.gov.uk/' + x for x in
                        re.findall('<a href="(.*?.xlsx)"', r.text)]
            # listods = re.findall('<a href="(.*?.ods)"', r.text)
        if len([listcsvs, listxls, listxlsx]) > 0:
            for filelocation in sum([listcsvs, listxls, listxlsx], []):
                try:
                    breakout = 0
                    for exclusion in exclusions:
                        if exclusion in str(filelocation):
                            # module_logger.info(
                                # os.path.basename(filelocation) + ' excluded.')
                            breakout = 1
                    if breakout == 1:
                        continue
                except Exception as e:
                    pass
                filename = os.path.basename(filelocation)
                while filename[0].isalpha() is False:
                    filename = filename[1:]
                if ('gpc' not in filename.lower()) \
                        and ('procurement' not in filename.lower()) \
                        and ('card' not in filename.lower()):
                    if os.path.exists(os.path.join(
                            os.path.join(filepath, department),
                            filename)) is False:
                        try:
                            r = requests.get(filelocation)
                        except Exception as e:
                            module_logger.debug('Problem downloading ' +
                                                ntpath.basename(filelocation) +
                                                ': ' + str(e))
                        with open(os.path.join(
                                  os.path.join(filepath, department),
                                  filename), "wb") as csvfile:
                            csvfile.write(r.content)
                        time.sleep(1.5)


def heading_replacer(columnlist, filepath):
    columnlist = [x if str(x) != 'nan' else 'dropme' for x in columnlist]
    columnlist = ['dropme' if str(x) == '-1.0' else x for x in columnlist]
    columnlist = [x if str(x) != '£' else 'amount' for x in columnlist]
    columnlist = [unidecode(x) for x in columnlist]
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
    removefields = pd.read_csv(os.path.join(
        filepath, '..', '..', 'support', 'remfields.csv'),
        names=['replacement'])['replacement'].values.tolist()
    for file_ in allFiles:
        if ntpath.basename(file_) in filestoskip:
            continue
        if file_.endswith('.ods'):
            module_logger.debug(ntpath.basename(file_) + ' is .ods: skipping')
            continue
        if os.path.getsize(file_) == 0:
            module_logger.debug(ntpath.basename(file_) + ' is 0b: skipping')
            continue
        if file_.endswith(tuple(['.csv', '.xls', '.xlsx'])) is False:
            module_logger.debug(ntpath.basename(file_) +
                                ': not csv, xls, xlsx or ods: not parsing....')
            continue
        try:
            if (file_.endswith('.xls')) or (file_.endswith('xlsx')):
                df = pd.read_excel(file_, index_col=None, encoding='latin-1',
                                   header=None, error_bad_lines=False,
                                   skip_blank_lines=True, warn_bad_lines=False)
            elif (file_.endswith('.csv')):
                df = pd.read_csv(file_, index_col=None, encoding='latin-1',
                                 header=None, error_bad_lines=False,
                                 skip_blank_lines=True, warn_bad_lines=False,
                                 engine='python')
            if ntpath.basename(file_) == 'DCMS_Transactions_over__25k_January_2016__1_.csv':
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
            if ntpath.basename(file_) == 'September_2013_Publishable_Spend_Over__25K_csv.csv':
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
            if department == 'dfeducation':
                try:
                    df = df[df['entity'] == 'DEPARTMENT FOR EDUCATION']
                except:
                    print(' the fuck')
            if len(df) > 0:
                try:
                    df['amount'] = df['amount'].astype(str).str.replace(
                        ',', '').str.extract('(\d+)',
                                             expand=False).astype(float)
                except Exception as e:
                    module_logger.debug("Can't convert amount to float in " +
                                        ntpath.basename(file_) + '. ')
                    print(df.columns.tolist())
                    print(ntpath.basename(file_))
                # try:
                #    duplicates = df[df.amount.duplicated() &
                #                    df.date.duplicated() &
                #                    df.supplier.duplicated()]
                #    if len(duplicates) > 0:
                #        df = df[~(df.amount.duplicated() &
                #                  df.date.duplicated() &
                #                  df.supplier.duplicated())]
                # except Exception as e:  # bare
                #    module_logger.debug('Problem with duplicates in ' +
                #                        ntpath.basename(file_) + '!')
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
            except:
                pass
            try:
                module_logger.debug('The first row: ' + str(df.iloc[0]))
            except:
                pass

    frame = pd.concat(list_)
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
    Most recent file: dec 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        base1 = 'department-for-education-and-executive-agency-spend-over-25000'
        base2 = 'department-for-education'
        dataloc = [pubs + 'dfe-and-executive-agency-spend-over-25000-2017-to-2018',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2016-to-2017',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2015-to-2016',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2014-to-2015',
                   pubs + base1 + '-financial-year-2013-to-2014',
                   pubs + '201213-' + base1,
                   pubs + 'departmental-and-alb-spend-over-25000-in-201112',
                   pubs + base2 + '-and-alb-spend-over-25000-201011']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def dohealth(filepath, dept):
    ''' Notes: collections with annual groupings, very logical annual subdomains
    Notes: However, department changes from DH to DHSC in end 2017 reshuffle.
    How to update: /collections/spending-over-25-000--2
    Most recent file: feb 2018
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
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def dftransport(filepath, dept):
    ''' Notes: everything in one data.gov.uk page, easy to scrape
    How to update: check dataset/financial-transactions-data-dft
    Most recent file: dec 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/financial-transactions-data-dft']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept, filestoskip=['dft-monthly-transparency-data-may-2017.csv',
                                                 'dft-monthly-transparency-data-nov-2016.xlsx',
                                                 'dft-monthly-transparency-data-sep-2016.xlsx'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def cabinetoffice(filepath, dept):
    ''' Notes: everything in one publications page
    How to update: publications/cabinet-office-spend-data
    Most recent file: feb 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'cabinet-office-spend-data']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def dfintdev(filepath, dept):
    ''' Notes: everything in one data.gov.uk page, easy. Threshold £500
    How to update: check dataset/financial-transactions-data-dft
    Most recent file: dec 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/spend-transactions-by-dfid']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept, filestoskip=['feb2015.csv',
                                                 'January2014.csv'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def dfinttrade(filepath, dept):
    ''' Notes: Dept only created in 2016. Groupled publications in collection
        for the most part of the first two years, then buggy/bad individual
        files hyperlinked to the collection page.
        How to update: check /collections/dit-departmental-spending-over-25000
        Most recent file: Feb 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'dit-spending-over-25000-february-2018',
                   pubs + 'dit-spending-over-25000-january-2018',
                   pubs + 'dit-spending-over-25000-december-2017',
                   pubs + 'dit-spending-over-25000-november-2017',
                   pubs + 'department-for-international-trade-spend-2017-to-2018',
                   pubs + 'department-for-international-trade-spend-2016-to-2017']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def dworkpen(filepath, dept):
    ''' Notes: Groupled publications in collection, but ends half through 2017
        How to update: check page at collections/dwp-payments-over-25-000
        Most recent file: May 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'dwp-payments-over-25000-for-2017',
                   pubs + 'dwp-payments-over-25000-for-2016',
                   pubs + 'dwp-payments-over-25000-for-2015',
                   pubs + 'publications/dwp-payments-over-25000-2014',
                   pubs + 'publications/dwp-payments-over-25-000-2013',
                   pubs + 'dwp-payments-over-25-000-2012',
                   pubs + 'dwp-payments-over-25-000-2011',
                   pubs + 'dwp-payments-over-25-000-2010']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def modef(filepath, dept):
    '''Notes: grouped £500 and £25000 together in one collection: super great.
    How to update: check collections/mod-finance-transparency-dataset
    Most recent file: Feb 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'mod-spending-over-25000-january-to-december-2014',
                   pubs + 'mod-spending-over-25000-january-to-december-2015',
                   pubs + 'mod-spending-over-25000-january-to-december-2016',
                   pubs + 'mod-spending-over-25000-january-to-december-2017',
                   pubs + 'mod-spending-over-25000-january-to-december-2018']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def mojust(filepath, dept):
    '''Notes: because there are so many MOJ arms length bodies, the collections
    are a bit weird. Slightly outdated alsoself.
    How to update: search for a new landing page, something akin to:
    collections/moj-spend-over-25000-2018?
    Most recent file: Sept 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'spend-over-25-000',  # really old data
                   pubs + 'ministry-of-justice-spend-over-25000-2013',
                   pubs + 'ministry-of-justice-spend-over-25000-2014',
                   pubs + 'ministry-of-justice-spend-over-25000-2015',
                   pubs + 'ministry-of-justice-spend-over-25000-2016',
                   pubs + 'ministry-of-justice-spend-over-25000-2017']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def dcultmedsport(filepath, dept):
    ''' Notes: This is quite a mess -- each pubs page has a differet annual set
    How to update: search for a new landing page, something akin to:
    publications/dcms-transactions-over-25000-201819?
    Most recent file: Jan 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'transactions-over-25k-2013-2014',
                   pubs + 'transactions-over-25000-august-2014',
                   pubs + 'transactions-over-25000-july-2014',
                   pubs + 'dcms-transactions-over-25000-2014-15',
                   pubs + 'dcms-transactions-over-25000-2015-16',
                   pubs + 'dcms-transactions-over-25000-201617',
                   pubs + 'dcms-transactions-over-25000-201718']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def ukexpfin(filepath, dept):
    ''' Notes: random files appear to be missing? good collection structure
    How to update: should be automatic? collections/ukef-spend-over-25-000
    Most recent file: Feb 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/ukef-spend-over-25-000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def dbusenind(filepath, dept):
    ''' Notes: very nice collection structure all on one pageself.
    How to update: collections/beis-spending-over-25000
    Most recent file: Sept 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/beis-spending-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def dfeeu(filepath, dept):
    ''' Notes: this is a bit of a mess - 5 month rolling csv files?
    How to update: search https://www.gov.uk/government/publications ?
    Most recent file: Feb 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        landing = 'department-for-exiting-the-european-union-spend-over-25000'
        month = '-april-2017-to-october-2017'
        get_data([pubs + landing + month], filepath, dept)
        month = '-november-2017-to-february-2018'
        get_data([pubs + landing + month], filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def foroff(filepath, dept):
    ''' Note: great: everything in one collection pageself.
    How to update: should be automatic, but if not check:
    collections/foreign-office-spend-over-25000
    Most recent file: Feb 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/foreign-office-spend-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def hmtreas(filepath, dept):
    ''' Note: out of date a bit, but collection is clean
    How to update: collections/25000-spend
    Most recent file: March 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/25000-spend')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def mhclg(filepath, dept):
    ''' Note this changes from dclg in dec 2017.
    Note: therefore, grab all MHCLG only... this is a broken mess in general
    How to update: collections/mhclg-departmental-spending-over-250
    Most recent file: February 2018
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
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


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
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


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
    Most recent file: Feb 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        land = 'departmental-spend-over-25000'
        SO = 'scotland-office-'
        htmllist = [land + '-january-2018', land + '-february-2018',
                    land + '-november-2017', land + '-december-2017',
                    land + '-october-2017', land + '-august-2017',
                    land + '-september-2017', land + '-april-2017',
                    land + '-march-2017', dept + '-february-2017',
                    SO + land + '-july-2017', SO + land + '-may-2017',
                    SO + land + '-may-2017', SO + land + '-june-2017',
                    land + '-may-2016', land + '-february-2016',
                    land + '-march-2016', land + '-april-2016',
                    land + '-july-2016', land + '-september-2016',
                    land + '-november-2016', land + '-january-2017',
                    land + '-june-2016', land + '-december-2016',
                    land + '-october-2016', land + '-december-2015',
                    land + '-april-2014--2', land + '-march-2014--2',
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
    df = parse_data(filepath, dept, filestoskip=['January__2018_-_Transparency-SO.csv',
                                                 'Departmental_spend_over__25_000_January_2017.csv',
                                                 'AUGUST_2011_20SO_20Transparency.csv',
                                                 'June_2017_-_Transparency_-_SO.csv',
                                                 'October_2017_-_Transparency-SO.csv',
                                                 'Spend-Over-25k-Jan-2012.csv',
                                                 'April_2014_transparency_OAG.csv',
                                                 'Februry__2018_-_Transparency-SO.csv'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def gldagohmcpsi(filepath, dept):
    ''' Notes: Data for Government Legal Department, Attorney General’s Office
    and HM Crown Prosecution Service Inspectorate. The final link on the
    page is everything prior to march 2017
    How to update: collections/gld-ago-hmcpsi-transactions-greater-than-25000
    Most recent file: Jan 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/' +
                         'gld-ago-hmcpsi-transactions-greater-than-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def homeoffice(filepath, dept):
    '''Note: all there, but gotta go into each annual page, not updated recently
    How to update: publications/home-office-spending-over-25000-2018 ?
    Most recent file: Jan 2017.
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'home-office-spending-over-25000-2017',
                   pubs + 'home-office-spending-over-25000-2016',
                   pubs + 'home-office-spending-over-25000-2015',
                   pubs + 'home-office-spending-over-25000-2014',
                   pubs + 'transparency-spend-over-25-000']
        get_data(dataloc, filepath, dept)
        if os.path.exists(os.path.join(filepath, dept, 'august-2012.xls')):
            os.remove(os.path.join(filepath, dept, 'august-2012.xls'))
    df = parse_data(filepath, dept, filestoskip=['april-2011.xls'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


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
        htmllist = [land + 'february-2018', land + 'december-2017',
                    land + 'november-2017', land + 'october-2017',
                    land + 'september-2017', land + 'august-2017',
                    land + 'july-2017', land + 'june-2017',
                    land + 'march-2017', land + 'february-2017',
                    land + 'january-2017', land + 'december-2016']
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
        r = requests.get(base + 'collections/spend-over-25-000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    df = parse_data(filepath, dept, filestoskip=['March_2017_-_Transparency_OAG.csv',
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
                                                 'Spend_over_25k_November_2012.xls'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


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
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def charcom(filepath, dept):
    ''' great: have to manually find pages in the search, slightly out of date
    How to update: manual search https://www.gov.uk/government/publications ?
    Most recent file: March 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        base1 = 'invoices-over-25k-during-financial-year-'
        dataloc = [pubs + base1 + '2011-2012', pubs + base1 + '2012-2013',
                   pubs + base1 + '2013-2014', pubs + base1 + '2014-15',
                   pubs + base1 + '2015-16', pubs + base1 + '2016-2017']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept, filestoskip=['May11.csv'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def commarauth():
    print('Commarauth has only .ods files: need to work on parsing these...')


def crownprosser(filepath, dept):
    ''' Note: custom function devised by the cps search function
    How to update: maybe add an extra page onto the range?
    Last update: Dec 2017?
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        crownbase = 'https://www.cps.gov.uk/search/'
        search = 'node?keys=CPS%20Expenditure%20over%20%C2%A325%2C000&page='
        dataloc = []
        for page in range(0, 6):
            r = requests.get(crownbase + search + str(page))
            dataloc = dataloc + \
                re.findall(
                    '<li><h3><a href=\"(.*?)\" target=\"_blank\">CPS', r.text)
            time.sleep(1.5)
        for file_ in dataloc:
            r = requests.get(file_)
            files = re.findall(
                'csv file--text\"><a href="(.*?)\" type=', r.text)
            for csv in files:
                if '.csv' in csv:
                    if os.path.exists(os.path.join(
                            os.path.join(filepath, dept),
                            csv.split('/')[-1])) is False:
                        r = requests.get(csv)
                        with open(os.path.join(os.path.join(filepath, dept),
                                               csv.split('/')[-1]),
                                  "wb") as csvfile:
                            csvfile.write(r.content)
                        time.sleep(1.5)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def fsa(filepath, dept):
    ''' some of the files requested are links to other sites and are returning
    html: but these dont get parsed so just ignore them for now '''
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
        for dataloc in datalocs:
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
                for filelocation in listcsvs:
                    filename = os.path.basename(filelocation)
                    if ('gpc' not in filename.lower() and '.csv' in filename.lower()):
                        if os.path.exists(os.path.join(
                                os.path.join(filepath, dept),
                                filename)) is False:
                            r = requests.get(filelocation)
                            with open(os.path.join(
                                      os.path.join(filepath, dept),
                                      filename), "wb") as csvfile:
                                csvfile.write(r.content)
                        time.sleep(1.5)
    df = parse_data(filepath, dept, filestoskip=['fsa-spend-aug2013.csv'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def forcomm(filepath, dept):
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        key = '7189ef00-0d1e-436e-bdb5-181519bccead'
        landingpage = '/spend-over-25-000-in-the-forestry-commission'
        r = requests.get(data + 'dataset/' + key + landingpage)
        soup = BeautifulSoup(r.text, 'lxml')
        listcsvs = []
        for link in soup.findAll('a', attrs={'href': re.compile("^http://")}):
            try:
                listcsvs.append(link.get('href'))
                if len(listcsvs) > 0:
                    for filelocation in listcsvs:
                        filename = os.path.basename(filelocation)
                        if ('gpc' not in filename.lower() and '.csv' in filename.lower()):
                            if os.path.exists(os.path.join(
                                    os.path.join(filepath, dept),
                                    filename)) is False:
                                r = requests.get(filelocation)
                                with open(os.path.join(
                                          os.path.join(filepath, dept),
                                          filename), "wb") as csvfile:
                                    csvfile.write(r.content)
                                time.sleep(1)
            except Exception as e:
                module_logger.info(dept + 'website down?')

    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)

    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def govlegdep():
    print('Data fpr GLD is merged with the AGO.')


def govaccdept(filepath, dept):
    ''' Note: each year has its own publications page, pretty janky
    How to update: search for 'gad-spend-greater-than-25000-2018?'?
    Most recent file: February 2018'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'gad-spend-greater-than-25000-2014',
                   pubs + 'gad-spend-greater-than-25000-2015',
                   pubs + 'gad-spend-greater-than-25000-2016',
                   pubs + 'gad-spend-greater-than-25000-2017',
                   pubs + 'gad-spend-greater-than-25000-2018']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept, filestoskip=['GAD_Nov_2016__25k_.csv'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def hmlandreg(filepath, dept):
    ''' Notes: exemplary! all in one collections page.
    How to update: should be automatic, if not, check:
    collections/land-registry-expenditure-over-25000
    Most recent file: February 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(
            base + 'collections/land-registry-expenditure-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def hmrc(filepath, dept):
    ''' Notes: exemplary! all in one collections page.
    How to update: should be automatic, if not, check:
    collections/spending-over-25-000
    Most recent file: February 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/spending-over-25-000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage],
                     filepath, dept, exclusions=['RCDTS'])
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def natsavinv(filepath, dept):
    '''Note: have to devise a less general function to visit third party website
    How to update: should be automatic, if not check the main landing page
    Most recent file: February 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        nsibase = 'http://nsandi-corporate.com/about-nsi/'
        nsisub = 'regulations-policies-and-procedures/nsi-transparency-reporting'
        r = requests.get(nsibase + nsisub + '/nsi-expenditure-over-25000/')
        htmllist = re.findall('<li><a class="csv" href="(.*?)\"', r.text)
        for file_ in htmllist:
            if '.csv' in file_:
                if 'http://nsandi-corporate.com/' not in file_:
                    file_ = 'http://nsandi-corporate.com/' + file_
                if os.path.exists(os.path.join(os.path.join(filepath, dept),
                                               file_.split('/')[-1])) is False:
                    r = requests.get(file_)
                    with open(os.path.join(os.path.join(filepath, dept),
                                           file_.split('/')[-1]), "wb") as csvfile:
                        csvfile.write(r.content)
                    time.sleep(1.5)
    df = parse_data(filepath, dept, filestoskip=[
        'transparency-25k-12-2014.csv'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def natarch(filepath, dept):
    ''' Note: complete listing on data.gov.uk.
    How to update: automatic?: dataset/national-archives-items-of-spending
    Most recent file: January 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/national-archives-items-of-spending']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept, filestoskip=[
        'april2013-spend-over10k.csv'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def natcrimag():
    print('The National Crime Agency is exempt from FOI stuffs.')


def offrailroad(filepath, dept):
    ''' Note: up to date listing on data.gov.uk, but starts late?
    How to update: dataset/office-of-rail-and-road-spending-over-25000-dataset
    Most recent file: Febriuary 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        landingpage = 'dataset/office-of-rail-and-road-spending-over-25000-dataset'
        dataloc = [data + landingpage]
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def ofgem(filepath, dept):
    ''' Note: custom function devised by the ofgem search function
    How to update: maybe add an extra page onto the range?
    Last update: Sept 2017?
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        ofgembase = 'https://www.ofgem.gov.uk/about-us/transparency/'
        search = 'expenses-and-expenditure/payments-suppliers?page='
        dataloc = []
        for page in range(1, 10):
            try:
                r = requests.get(ofgembase + search + str(page))
                dataloc = dataloc + \
                    re.findall('field-content"><a href="(.*?)"><span>', r.text)
            except Exception as e:
                module_logger .info(dept + 'website down?')
                break
            time.sleep(1)
    if 'noscrape' not in sys.argv:
        for file_ in dataloc:
            r = requests.get('https://www.ofgem.gov.uk/' + file_)
            time.sleep(1.5)
            files = re.findall(
                'file-container"><a href=\"(.*?)\" onclick=\"', r.text)
            for csv in files:
                if '.csv' in csv:
                    if os.path.exists(os.path.join(os.path.join(filepath, dept),
                                                   csv.split('/')[-1])) is False:
                        r = requests.get(csv)
                        with open(os.path.join(os.path.join(filepath, dept),
                                               csv.split('/')[-1]), "wb") as csvfile:
                            csvfile.write(r.content)
                        time.sleep(1.5)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def ofqual(filepath, dept):
    '''Notes: Everything in one publications sheet
    How to update: automatic? publications/ofqual-spend-data-over-500
    Most recent file: October 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'ofqual-spend-data-over-500']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept, filestoskip=['Ofqual_Expenditure_over_25k_May_2016.csv',
                                                 'Ofqual_Expenditure_over_25k_January_2017.csv',
                                                 'Ofqual_Expenditure_over_25k_November_2016.csv',
                                                 'Ofqual_Expenditure_over_25k_July_2013.csv',
                                                 'Ofqual_Expenditure_over_25k_October_2016.csv'])
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def ofsted(filepath, dept):
    '''Notes: four publications pages linked together via a collection
    How to update: collections/ofsted-spending-over-25000
    Most recent file: February 2018'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'ofsted-spending-over-25000-since-april-2010',
                   pubs + 'ofsted-spending-over-25000-2016',
                   pubs + 'ofsted-spending-over-25000-2017',
                   pubs + 'ofsted-spending-over-25000-2018']
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def serfraud(filepath, dept):
    '''' need to check this: not all links are being returned: some lead on
    and something is broken i fear'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        key = 'fd574314-3674-48d0-a139-624d6c5405e1'
        landingpage = '/spend-over-25-000-in-the-serious-fraud-office'
        r = requests.get(data + 'dataset/' + key + landingpage)
        soup = BeautifulSoup(r.text, 'lxml')
        listcsvs = []
        for link in soup.findAll('a', attrs={'href': re.compile("^http://")}):
            listcsvs.append(link.get('href'))
            if len(listcsvs) > 0:
                for filelocation in listcsvs:
                    filename = os.path.basename(filelocation)
                    if ('gpc' not in filename.lower()) and ('.csv' in filename.lower()):
                        if os.path.exists(os.path.join(
                                os.path.join(filepath, dept),
                                filename)) is False:
                            r = requests.get(filelocation)
                            with open(os.path.join(
                                      os.path.join(filepath, dept),
                                      filename), "wb") as csvfile:
                                csvfile.write(r.content)
                            time.sleep(1)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def supcourt(filepath, dept):
    '''Notes: have to go via a third party website and create a custom function
    but otherwise seems to be working fine and well. Nicely grouped annually.
    How to update: check for existance of 2019.csv at:
        https://www.supremecourt.uk/about/transparency.html
    Most recent file: annual 2018 file -- specifies when most recently updated
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        SCbase = 'https://www.supremecourt.uk/docs/transparency-transactions-'
        htmllist = [SCbase + '2010.csv', SCbase + '2011.csv', SCbase + '2012.csv',
                    SCbase + '2013.csv', SCbase + '2014.csv', SCbase + '2015.csv',
                    SCbase + '2016.csv', SCbase + '2017.csv', SCbase + '2018.csv']
        for html_ in htmllist:
            r = requests.get(html_)
            with open(os.path.join(os.path.join(filepath, dept),
                                   html_.split('/')[-1]),
                      "wb") as csvfile:
                csvfile.write(r.content)
            time.sleep(1)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def ukstatauth():
    print('Cannot find data for the UK Statistics Authority...')


def ofwat(filepath, dept):
    ''' Note: complete listing on data.gov.uk. some deadlinks...
    How to update: automatic?: dataset/financial-transactions-data-ofwat
    Most recent file: January 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        key = '43e2236d-f00a-4762-929d-2211e0ab5ad8/'
        landingpage = 'spend-over-25-000-in-ofwat'
        dataloc = [data + 'dataset/' + key + landingpage]
        get_data(dataloc, filepath, dept, exclusions=[
                 'prs_dat_transactions201107'])
    df = parse_data(filepath, dept, filestoskip=['prs_dat_transactions201008.csv',
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


def build_merged(rawpath):
    ''' build merged databases'''
    print('\n>> Now working on Constructing Merged Departments!\n')
    filecountstart = sum([len(files) for r, d, files in os.walk(
        os.path.join(rawpath, 'Ministerial'))])
    if 'depttype=nonministerial' not in sys.argv:
        modef(os.path.join(rawpath, 'Ministerial'), 'modef')
        cabinetoffice(os.path.join(rawpath, 'Ministerial'), 'cabinetoffice')
        dftransport(os.path.join(rawpath, 'Ministerial'), 'dftransport')
        dohealth(os.path.join(rawpath, 'Ministerial'), 'dohealth')
        dfeducation(os.path.join(rawpath, 'Ministerial'), 'dfeducation')
        dfintdev(os.path.join(rawpath, 'Ministerial'), 'dfintdev')
        dfinttrade(os.path.join(rawpath, 'Ministerial'), 'dfinttrade')
        dworkpen(os.path.join(rawpath, 'Ministerial'), 'dworkpen')
        mojust(os.path.join(rawpath, 'Ministerial'), 'mojust')
        dcultmedsport(os.path.join(rawpath, 'Ministerial'), 'dcultmedsport')
        ukexpfin(os.path.join(rawpath, 'Ministerial'), 'ukexpfin')
        dbusenind(os.path.join(rawpath, 'Ministerial'), 'dbusenind')
        dfeeu(os.path.join(rawpath, 'Ministerial'), 'dfeeu')
        foroff(os.path.join(rawpath, 'Ministerial'), 'foroff')
        hmtreas(os.path.join(rawpath, 'Ministerial'), 'hmtreas')
        mhclg(os.path.join(rawpath, 'Ministerial'), 'mhclg')
        nioff(os.path.join(rawpath, 'Ministerial'), 'nioff')
        waleoff()
        scotoff(os.path.join(rawpath, 'Ministerial'), 'scotoff')
        gldagohmcpsi(os.path.join(rawpath, 'Ministerial'), 'gldagohmcpsi')
        homeoffice(os.path.join(rawpath, 'Ministerial'), 'homeoffice')
        leaderlords()
        leadercommons()
        oags(os.path.join(rawpath, 'Ministerial'), 'oags')
        defra(os.path.join(rawpath, 'Ministerial'), 'defra')
    if 'depttype=ministerial' not in sys.argv:
        charcom(os.path.join(rawpath, 'NonMinisterial'), 'charcom')
        commarauth()
        crownprosser(os.path.join(rawpath, 'NonMinisterial'), 'crownprosser')
        fsa(os.path.join(rawpath, 'NonMinisterial'), 'fsa')
        forcomm(os.path.join(rawpath, 'NonMinisterial'), 'forcomm')
        govlegdep()
        govaccdept(os.path.join(rawpath, 'NonMinisterial'), 'govaccdept')
        hmlandreg(os.path.join(rawpath, 'NonMinisterial'), 'hmlandreg')
        hmrc(os.path.join(rawpath, 'NonMinisterial'), 'hmrc')
        natsavinv(os.path.join(rawpath, 'NonMinisterial'), 'natsavinv')
        natarch(os.path.join(rawpath, 'NonMinisterial'), 'natarch')
        natcrimag()
        offrailroad(os.path.join(rawpath, 'NonMinisterial'), 'offrailroad')
        ofgem(os.path.join(rawpath, 'NonMinisterial'), 'ofgem')
        ofqual(os.path.join(rawpath, 'NonMinisterial'), 'ofqual')
        ofsted(os.path.join(rawpath, 'NonMinisterial'), 'ofsted')
        serfraud(os.path.join(rawpath, 'NonMinisterial'), 'serfraud')
        supcourt(os.path.join(rawpath, 'NonMinisterial'), 'supcourt')
        ukstatauth()
        ofwat(os.path.join(rawpath, 'NonMinisterial'), 'ofwat')
    filecountend = sum([len(files) for r, d, files in os.walk(
        os.path.join(rawpath, 'Ministerial'))])
    print('Added a total of ' + str(filecountend - filecountstart) +
          ' new files.')
