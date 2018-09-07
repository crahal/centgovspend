'''
Options:    depttype=ministerial    : only scrape/parse ministerial depts
            depttype=nonministerial : only scrape/parse nonministerial depts
            cleanrun                : delete all subdirectories before running
                                      (default = off)
            noscrape                : dont scrape any new data
                                      (incompatabile \w cleanrun, default off)
            noreconcile             : dont match and merge with opencorporates,
                                      companies house, or (later) opencalais
                                      (default = do it)
Links last updated: 10/04/2018
Links next updated: 01/07/2018.

'''

import os
import sys
import shutil
import logging
from datetime import datetime
import pandas as pd
from scrape_and_parse import build_merged, merge_files
from evaluation import evaluate_and_clean_merge, evaluate_reconcile
from reconcile import reconcile_dataframe


def start_banner():
    print('**************************************************')
    print('*********Welcome to centgovspend v.1.0.0!*********')
    print('**************************************************')
    print('*** This is an extremely preliminary version.*****')
    print('***       Please raise issues on GitHub!     *****')
    print('***       Started at ' +
          str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + '     *****')
    print('**************************************************')


def end_banner():
    print('\n**************************************************')
    print('**** Program finished at ' +
          str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + '  ****')
    print('**************************************************')


if __name__ == '__main__':
    start_banner()
    if ('cleanrun' in sys.argv) and ('noscrape' in sys.argv):
        print('Incompatabile options specified! Exiting')
        quit()
    rawpath = os.path.abspath(os.path.join(__file__, '../..', 'data', 'raw'))
    logpath = os.path.abspath(os.path.join(__file__, '../..', 'logging'))
    if os.path.exists(logpath):
        if os.path.isfile(os.path.abspath(
                          os.path.join(logpath, 'centgovspend.log'))):
            os.remove(os.path.abspath(
                      os.path.join(logpath, 'centgovspend.log')))
    else:
        os.makedirs(logpath)
    logger = logging.getLogger('centgovspend_application')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler((os.path.abspath(
        os.path.join(__file__, '../..', 'logging',
                     'centgovspend.log'))))
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    if 'cleanrun' in sys.argv:
        try:
            shutil.rmtree(os.path.join(rawpath, 'ministerial'))
            shutil.rmtree(os.path.join(rawpath, 'nonministerial'))
            print('*** Doing a clean run! Lets go! ***')
        except Exception as e:
            logger.info('cleanrun option passed, but cannot delete folders.')
    if os.path.exists(rawpath) is False:
        os.makedirs(rawpath)
    build_merged(rawpath)
    All_Merged_Unmatched = merge_files(rawpath)
    All_Merged_Unmatched = evaluate_and_clean_merge(All_Merged_Unmatched,
                                                    rawpath)
    if 'noreconcile' not in sys.argv:
        uniquesups = pd.DataFrame(All_Merged_Unmatched['supplier_upper'].unique(),
                                  columns=['supplier_upper'])
        reconcile_dataframe(rawpath, uniquesups)
        evaluate_reconcile(rawpath)
    logging.shutdown()
    end_banner()
