import pandas as pd


def clean_matches(df, match_type):
    '''Note: centgovspend assumes type=automated_safe.
    If you want to automate this library to work with manual_verification,
    then work needs to be done in rewriting the reconcile module to get all
    auxillery data from CHI API relating to secondary matches.'''

    if match_type == 'automated_safe':
        print('***Undertaking an automated safe reconciliation!***')
        df = df[df['First Score'] > 70]
        df = df[(df['First Score'] - df['Second Score'] >= 10) |
                (pd.isnull(df['Second Score']))]
    elif match_type == 'manual_verification':
        df = df[df['First Score'] > 0]
        df = df.reset_index(drop=True)
        print('Undertaking an manual reconciliation: we have ' +
              str(len(df[df['First Score'] < 70])) + ' matches to make!')
        for index, row in df.iterrows():
            if row['First Score'] >= 70:
                df.loc[index, 'Best ID'] = row['First ID']
                df.loc[index, 'Best Match'] = row['First Match']
            else:
                x = ''
                z = ''
                while (x.upper() != 'Y'):
                    x = input(str(round(((index+1)/len(df))*100, 2)) +
                              '% Finished. Supplier: ' +
                              row['RawSupplier'] + '. Suggestion: ' +
                              row['First Match'] + '. Accept? Y/N only.')
                    if x.upper() == 'Y':
                        df.loc[index, 'Best ID'] = row['First ID']
                        df.loc[index, 'Best Match'] = row['First Match']
                    elif x.upper() == 'N':
                        while (z.upper() != 'Y'):
                            if isinstance(row['Second Match'], str) is True:
                                z = input(str(round(((index+1)/len(df))*100, 2)) +
                                          'Supplier: ' +
                                          row['RawSupplier'] + '. Suggestion: ' +
                                          row['Second Match'] + '. Accept? Y/N only.')
                                if z.upper() == 'Y':
                                    df.loc[index, 'Best ID'] = row['Second ID']
                                    df.loc[index, 'Best Match'] = row['Second Match']
                                else:
                                    df.loc[index, 'Best ID'] = 'No Match'
                                    df.loc[index, 'Best Match'] = 'No Match'
                                    z = 'Y'
                            else:
                                df.loc[index, 'Best ID'] = 'No Match'
                                df.loc[index, 'Best Match'] = 'No Match'
                                z = 'Y'
                        x = 'Y'
        df = df[df['Best Match'] != 'No Match']
    else:
        print('Please specify "automated_safe" or "manual_verification"!')
    return df
