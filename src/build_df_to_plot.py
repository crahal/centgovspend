import glob
import ntpath
import pandas as pd


def build_timely_df(payments):
    ''' build the longitudinal dataset for figure 2b '''
    timely_df = pd.DataFrame(columns=['Number of Payments',
                                      'Value of Payments'])
    for year in list(range(2009, 2018)):
        timely_df.at[str(year) + 'Q1', 'Number of Payments'] = len(payments[
            (payments['date'].dt.month <= 3) &
            (payments['date'].dt.year == year)])/1000
        timely_df.at[str(year) + 'Q1', 'Value of Payments'] = payments[
            (payments['date'].dt.month <= 3) &
            (payments['date'].dt.year == year)]['amount'].sum()/1000000000
        timely_df.at[str(year) + 'Q2', 'Number of Payments'] = len(payments[
            (3 < payments['date'].dt.month) &
            (payments['date'].dt.month <= 6) &
            (payments['date'].dt.year == year)])/1000
        timely_df.at[str(year) + 'Q2', 'Value of Payments'] = payments[
            (3 < payments['date'].dt.month) &
            (payments['date'].dt.month <= 6) &
            (payments['date'].dt.year == year)]['amount'].sum()/1000000000
        timely_df.at[str(year) + 'Q3', 'Number of Payments'] = len(payments[
            (6 < payments['date'].dt.month) &
            (payments['date'].dt.month <= 9) &
            (payments['date'].dt.year == year)])/1000
        timely_df.at[str(year) + 'Q3', 'Value of Payments'] = payments[
            (6 < payments['date'].dt.month) &
            (payments['date'].dt.month <= 9) &
            (payments['date'].dt.year == year)]['amount'].sum()/1000000000
        timely_df.at[str(year) + 'Q4', 'Number of Payments'] = len(payments[
            (9 < payments['date'].dt.month) &
            (payments['date'].dt.month <= 12) &
            (payments['date'].dt.year == year)])/1000
        timely_df.at[str(year) + 'Q4', 'Value of Payments'] = payments[
            (9 < payments['date'].dt.month) &
            (payments['date'].dt.month <= 12) &
            (payments['date'].dt.year == year)]['amount'].sum()/1000000000
    timley_resetindex = timely_df.reset_index()
    return timley_resetindex, timely_df


def build_pesa_df(pesa_path, merged_path):
    ''' build a df comparing centgovspend to pesa '''
    pesa_table = pd.read_csv(pesa_path, index_col='Department')
    list_of_files = []
    for file_ in glob.glob(merged_path):
        if ntpath.basename(file_)[:-4] in pesa_table.index:
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
            cols_to_consider = ['amount', 'date', 'dept', 'expensearea',
                                'expensetype', 'transactionnumber', 'supplier']
            grouped = df.groupby(cols_to_consider)
            index = [gp_keys[0] for gp_keys in grouped.groups.values()]
            df_clean = df.reindex(index)
            pesa_table.at[ntpath.basename(file_)[:-4],
                          'Raw_total_value'] = df_clean.amount.sum()
            pesa_table.at[ntpath.basename(file_)[:-4],
                          'Raw_total_number'] = len(df_clean.amount)
            df_clean['date_dt'] = pd.to_datetime(df_clean['date'],
                                                 errors='coerce')
            df_temp = df_clean[(df_clean['date_dt'] > '2016-01-04') &
                               (df_clean['date_dt'] < '2017-03-31')]
            pesa_table.at[ntpath.basename(file_)[:-4],
                          'Raw_20162017_value'] = df_temp['amount'].sum()
            pesa_table.at[ntpath.basename(file_)[:-4],
                          'Raw_20162017_number'] = len(df_temp)
            pesa_table.at[ntpath.basename(file_)[:-4],
                          'Raw_20162017_filecount'] = len(df_temp['file'].
                                                          unique())
            df_temp = df_clean[(df_clean['date_dt'] > '2017-01-04') &
                               (df_clean['date_dt'] < '2018-03-31')]
            pesa_table.at[ntpath.basename(file_)[:-4],
                          'Raw_20172018_value'] = df_temp['amount'].sum()
            pesa_table.at[ntpath.basename(file_)[:-4],
                          'Raw_20172018_number'] = len(df_temp)
            pesa_table.at[ntpath.basename(file_)[:-4],
                          'Raw_20172018_filecount'] = len(df_temp['file'].
                                                          unique())
            tempdf = pd.merge(pd.DataFrame(df.groupby(['file']).size()).
                              rename({0: 'full_count'}, axis=1),
                              pd.DataFrame(df_clean[df_clean['date_dt'].
                                                    isnull()].
                                           groupby(['file']).size()).
                              rename({0: 'nulldate_count'}, axis=1),
                              how='left', left_index=True, right_index=True)
            tempdf['dept'] = ntpath.basename(file_)[:-4]
            list_of_files.append(tempdf)
    list_of_files = pd.concat(list_of_files, sort=False)
    pesa_table['Raw_20162017_value'] = pesa_table['Raw_20162017_value']/1000000
    pesa_table['Raw_20172018_value'] = pesa_table['Raw_20172018_value']/1000000
    pesa_table = pesa_table.rename({'hmtreas': 'HMT',
                                    'dfinttrade': 'DIT',
                                    'foroff': 'FO',
                                    'defra': 'DEFRA',
                                    'dcultmedsport': 'DCMS',
                                    'cabinetoffice': 'CO',
                                    'dfintdev': 'DID',
                                    'mojust': 'MOJ',
                                    'homeoffice': 'HO',
                                    'dbusenind': 'BEIS',
                                    'dftransport': 'DT',
                                    'mhclg': 'MHCLG',
                                    'modef': 'MoD',
                                    'hmrc': 'HMRC',
                                    'dfeducation': 'DfE',
                                    'dohealth': 'DoH',
                                    'dworkpen': 'DWP'},
                                   axis=0)
    pesa_table['20172018Budget'] = pesa_table['20172018Budget']/1000
    pesa_table['Raw_20172018_value'] = pesa_table['Raw_20172018_value']/1000
    pesa_table['Ratio_20172018'] = pd.to_numeric(pesa_table['Raw_20172018_value']/pesa_table['20172018Budget'])
    return pesa_table, list_of_files


def trillions(x, pos):
    return '£%1.1ftrn' % (x)


def billions(x, pos):
    return '£%1.0fbn' % (x)


def thousands(x, pos):
    return '%1.0fk' % (x)


def clean_occupations(x):
    x = x.replace('.', '')
    x = x.replace(',', '')
    return x.title()


def clean_officer_names(x):
    try:
        if len(x.split(' ')) >= 1:
            return x.split(' ')[1]
    except IndexError:
        pass


def clean_countries(x):
    x = x.lower().replace('england', 'united kingdom')
    x = x.lower().replace('scotland', 'united kingdom')
    x = x.lower().replace('wales', 'united kingdom')
    x = x.lower().replace('northern ireland', 'united kingdom')
    x = x.lower().replace('britain', 'united kingdom')
    x = x.lower().replace('.', '')
    x = x.lower().replace(',', '')
    if x.lower()[0:2] == 'gb':
        x = 'united kingdom'
    return x.title()


def clean_nationalities(x):
    x = x.lower().replace('english', 'british')
    x = x.lower().replace('scottish', 'british')
    x = x.lower().replace('welsh', 'british')
    x = x.lower().replace('northern irish', 'british')
    x = x.lower().replace('united kingdom', 'british')
    x = x.lower().replace('.', '')
    x = x.lower().replace(',', '')
    if x.lower()[0:2] == 'uk':
        x = 'british'
    return x.title()
