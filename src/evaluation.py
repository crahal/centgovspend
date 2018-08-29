import os
import pandas as pd


def evaluate_and_clean_merge(df, rawpath):

    df = df.reset_index().drop('index', axis=1)
    print('\n** Evaluating the merged dataset and' +
          ' sequentially cleaning it!**')
    print('** We have ' + str(len(df)) +
          ' total rows of payments data to begin with.')
    print('** We have £' + str(round(df['amount'].sum()/1000000000, 2)) +
          'bn worth of data to begin with.')
    print('** We have ' + str(len(df['supplier'].unique())) +
          ' unique suppliers to begin with.')
    originallength = len(df)
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df = df[~pd.isnull(df['amount'])]
    df = df[~pd.isnull(df['supplier'])]
    df['supplier'] = df['supplier'].str.replace('\n', ' ').replace('\r', ' ')
    df['supplier'] = df['supplier'].str.strip()
    df = df[df['supplier'] != '']
    df = df[df['supplier'].str.len() > 3]
    df = df[(df['supplier'].notnull()) &
            (df['amount'].notnull())]
    print('Dropped ' + str(originallength - len(df)) +
          ' rows due bad supplier, amount or dates.')

    originallength = len(df)
    poss_redacts = ['redacted', 'redaction', 'xxxxxx', 'named individual',
                    'personal expense', 'name withheld', 'name removed']
    initial = len(df)
    valuefull = df['amount'].sum()
    numsups = len(df['supplier'].unique())
    df['expensetype'] = df['expensetype'].astype(str)
    df['expensetype'] = df['expensetype'].str.lower()
    df['expensetype'] = df['expensetype'].str.strip()
    df['expensetype'] = df['expensearea'].astype(str)
    df['expensetype'] = df['expensearea'].str.lower()
    df['expensetype'] = df['expensearea'].str.strip()
    df['supplier'] = df['supplier'].astype(str)
    df['supplier'] = df['supplier'].str.strip()
    for term in poss_redacts:
        df = df[~df['supplier'].str.contains(term, na=False)]
        df = df[~df['expensetype'].str.contains(term, na=False)]
        df = df[~df['expensearea'].str.contains(term, na=False)]
    print('Dropped ' + str(initial - len(df)) +
          ' redacted payments.')
    print('Dropped redacted payments worth £' +
          str(round((valuefull - df['amount'].sum())/1000000000, 2)) + 'bn.')
    print('We identified ' +
          str(numsups - len(df['supplier'].unique())) +
          ' unique redacted supplier variations.')

    initial = len(df)
    valuefull = df['amount'].sum()
    df = df[df['amount'] >= 25000]
    print('Dropped ' + str(initial - len(df)) +
          ' payments below £25k in value.')
    print('Dropped £' +
          str(round((valuefull - df['amount'].sum())/1000000000, 2)) +
          'bn worth of payments below £25k in value.')

    initial = len(df)
    numsups = len(df['supplier'].unique())
    valuefull = df['amount'].sum()
    df = df[df['supplier'].str.lower() != 'various']
    print('Dropped ' + str(initial - len(df)) +
          ' "various" payments.')
    print('Dropped "various" payments worth £' +
          str(round((valuefull - df['amount'].sum())/1000000000, 2)) + 'bn.')
    print('We identified ' +
          str(numsups - len(df['supplier'].unique())) +
          ' unique "various" supplier strings.')

    cols_to_consider = ['amount', 'date', 'dept', 'expensearea',
                        'expensetype', 'transactionnumber', 'supplier']
    grouped = df.groupby(cols_to_consider)
    index = [gp_keys[0] for gp_keys in grouped.groups.values()]
    df_clean = df.reindex(index)
    print('Dropped ' + str(len(df) - len(df_clean)) + ' potential duplicates')
    print('Dropped duplicates worth £' + str(round(df_clean['amount'].sum() /
          1000000000, 2)) + 'bn.')

    print('** We have ' + str(len(df)) + ' total rows of data to finish with.')
    print('** We have £' + str(round(df['amount'].sum()/1000000000, 2)) +
          'bn worth of data to finish with.')
    print('** We have ' + str(len(df['supplier'].unique())) +
          ' unique suppliers to finish with.')
    print('** We merge from across ' + str(len(df_clean['dept'].unique())) +
          ' departments.')
    print('** This data comes from: ' +
          str(len(df_clean['file'].unique())) + ' files.')
    df_clean.to_csv(os.path.join(rawpath, '..', 'output',
                                 'master', 'All_Merged_Unmatched.csv'),
                    index=False)
    print('Cleaned file at: ' +
          str(os.path.join('output', 'All_Merged_Unmatched.csv')))
    return df_clean


def evaluate_reconcile(rawpath):
    print('\n**************************************************')
    print('************Evalulting Reconciliations************')
    print('**************************************************')
    payments = pd.read_csv(os.path.abspath(
        os.path.join('__file__', '../..', 'data', 'output', 'master',
                     'All_Merged_Unmatched.csv')),
        encoding="ISO-8859-1", sep=',', engine='python',
        dtype={'transactionnumber': str,
               'supplier': str,
               'date': str,
               'expensearea': str,
               'expensetype': str,
               'file': str})
    payments['amount'] = pd.to_numeric(payments['amount'], errors='coerce')

    recon_sup = pd.read_csv(os.path.abspath(
        os.path.join('__file__', '../..', 'data', 'output', 'master',
                     'Reconciled_Suppliers.tsv')),
        encoding="ISO-8859-1", sep='\t')

    reconciled = pd.merge(payments, recon_sup, how='left', left_on='supplier',
                          right_on='RawSupplier')
    reconciled['amount'] = pd.to_numeric(reconciled['amount'], errors='coerce')

    matched = reconciled[~pd.isnull(reconciled['Best Match'])]

    print('We matched ' + str(int(len(matched))) + ' payments.')
    print('This is out of ' + str(int(len(payments))) + ' total.')
    print('This represents ' + str(round(((len(matched) /
                                           len(payments)) * 100), 2)) +
          '%.\n')
    print('We matched £' + str(int(matched['amount'].sum())) + ' of value.')
    print('This is out of £' +
          str(int(payments['amount'].sum())) + ' total value.')
    print('This represents ' + str(round(((matched['amount'].sum() /
                                           payments['amount'].sum()) *
                                          100), 2)) + '%.\n')
    print('We matched ' +
          str(int(len(matched['supplier'].unique()))) + ' suppliers.')
    print('This is out of ' +
          str(int(len(payments['supplier'].unique()))) + ' in total.')
    print('This represents ' + str(round(((len(matched['supplier'].unique()) /
                                           len(payments['supplier'].unique())) *
                                          100), 2)) + '%.')
