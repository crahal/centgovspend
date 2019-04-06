# -*- coding: utf-8 -*-
"""
Created on Mon Jan 21 20:02:23 2019

This script downloads all files from companies house and parses the xbrl files
into a csv (one per bulk monthly download). It leverages the ixbrl parser
(in dk_ixbrl_parser) written by David Kane, to which full attribution is made.
"""

from dk_ixbrl_parser import IXBRL
from bs4 import BeautifulSoup
import requests
import os
import zipfile
import csv
from tqdm import tqdm

rootdir = os.path.abspath(os.path.join(__file__,
                                       '../../..',
                                       'CH_Data',
                                       'accounts_data'))
baseurl = 'http://download.companieshouse.gov.uk/'
url = baseurl + 'en_monthlyaccountsdata.html'


def main():
    names_to_keep = ['CurrentAssets',
                     'UKCompaniesHouseRegisteredNumber',
                     'NetCurrentAssetsLiabilities',
                     'TotalAssetsLessCurrentLiabilities',
                     'ProfitLoss',
                     'PropertyPlantEquipmentGrossCost',
                     'FixedAssets',
                     'AverageNumberEmployeesDuringPeriod']
    html = requests.get(url).text
    soup = BeautifulSoup(html, features='lxml')
    for name in soup.findAll('a', href=True):
        if name['href'].endswith('.zip'):
            print('Now working on %s.' % name['href'])
            outfname = os.path.join(rootdir, name['href'])
            if os.path.isfile(outfname) is False:
                r = requests.get(baseurl + name['href'], stream=True)
                if r.status_code == requests.codes.ok:
                    print('Downloading %s' % name['href'])
                    with open(outfname, 'wb') as fd:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:
                                fd.write(chunk)
                        fd.close()
            if os.path.isfile(os.path.join(
                    rootdir, name['href']).split('.')[0]+'.csv') is False:
                with zipfile.ZipFile(os.path.join(rootdir,
                                                  name['href'])) as zipper:
                    pbar = tqdm(zipper.namelist(),
                                desc="Transfer progress",
                                ncols=1)
                    for file in pbar:
                        if file.endswith('.html'):
                            with zipper.open(file) as fp:
                                try:
                                    x = IXBRL(fp)
                                except Exception as e:
                                    pass
                                try:
                                    vals = x.to_table("all")
                                    vals = [dict(i,
                                                 file_name=file) for i in vals]
                                    keeplist = []
                                    list_to_drop = ['schema',
                                                    'segment:0',
                                                    'startdate',
                                                    'instant',
                                                    'enddate']
                                    for ii in range(len(vals)):
                                        if (vals[ii]['name'].
                                            replace('[^a-zA-Z]',
                                                    '') in names_to_keep)\
                                                    and (('2017' in\
                                                         str(vals[ii]['enddate'])) or
                                                         ('2017' in\
                                                              str(vals[ii]['instant']))):
                                            for dropper in list_to_drop:
                                                if dropper in vals[ii]:
                                                    del vals[ii][dropper]
                                            keeplist.append(vals[ii])
                                except Exception as e:
                                    tqdm.write('Error cleaning list: ',
                                               str(e))
                                    pass
                                try:
                                    with open(os.path.join(
                                            rootdir,
                                            name['href']).split('.')[0]+'.csv',
                                              'a',
                                              encoding="utf8") as fou:
                                        w = csv.DictWriter(fou,
                                                           keeplist[0].keys(),
                                                           lineterminator='\n',
                                                           extrasaction='ignore')
                                        if fou.tell() == 0:
                                            w.writeheader()
                                        w.writerows(keeplist)
                                except Exception as e:
                                    pass
            try:
                os.remove(outfname)
            except Exception as e:
                pass


if __name__ == "__main__":
    main()
