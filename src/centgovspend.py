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
from scrape_and_parse import build_merged, merge_files
from evaluation import evaluate_and_clean_merge, evaluate_reconcile
from reconcile import reconcile_dataframe

if __name__ == '__main__':
    print('**************************************************')
    print('*********Welcome to centgovspend v.0.1.0!*********')
    print('**************************************************')
    print('*** This is an extremely preliminary version.*****')
    print('***       Please raise issues on GitHub!     *****')
    print('***       Started at ' +
          str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + '     *****')
    print('**************************************************')
    if ('cleanrun' in sys.argv) and ('noscrape' in sys.argv):
        print('Incompatabile options specified! Exiting')
        quit()
    rawpath = os.path.abspath(os.path.join(__file__, '../..', 'data', 'raw'))
    if os.path.exists(os.path.abspath(
            os.path.join(__file__, '../..', 'logging'))):
        if os.path.isfile(os.path.abspath(
                          os.path.join(__file__, '../..', 'logging',
                                       'centgovspend.log'))):
            os.remove(os.path.abspath(
                      os.path.join(__file__, '../..', 'logging',
                                   'centgovspend.log')))
    else:
        os.makedirs(os.path.abspath(
            os.path.join(__file__, '../..', 'logging')))
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

    All_Merged_Unmatched = evaluate_and_clean_merge(
        All_Merged_Unmatched, rawpath)
    #if 'noreconcile' not in sys.argv:
    #    uniquesups = pd.DataFrame(All_Merged_Unmatched['supplier'].unique(),
    #                              columns=['supplier'])
    #    reconcile_dataframe(rawpath, uniquesups)
    #    evaluate_reconcile(rawpath)
    logging.shutdown()

    print('\n**************************************************')
    print('**** Program finished at ' +
          str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + '  ****')
    print('**************************************************')
